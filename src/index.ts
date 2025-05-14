/**
 * Welcome to Cloudflare Workers! This is your first worker.
 *
 * - Run `npm run dev` in your terminal to start a development server
 * - Open a browser tab at http://localhost:8787/ to see your worker in action
 * - Run `npm run deploy` to publish your worker
 *
 * Bind resources to your worker in `wrangler.jsonc`. After adding bindings, a type definition for the
 * `Env` object can be regenerated with `npm run cf-typegen`.
 *
 * Learn more at https://developers.cloudflare.com/workers/
 */

import bencode from "bencode";

export interface Env {
	BUCKET: R2Bucket;
}

// Constants for Torrent Creation
const TORRENT_PIECE_LENGTH = 262144; // 256 KiB
const ANNOUNCE_URL = "udp://tracker.openbittorrent.com:6969/announce"; // Example public tracker

export default {
	async fetch(req: Request, env: Env): Promise<Response> {
		const url = new URL(req.url);
		console.log("Received req.url:", req.url);

		const repo = url.searchParams.get("repo");
		const rev = "main";

		if (!repo) return new Response("missing ?repo=", { status: 400 });

		const hfApiUrl = `https://huggingface.co/api/models/${repo}/tree/${rev}`;
		const apiResponse = await fetch(hfApiUrl);
		if (!apiResponse.ok) {
			return new Response(`HF API error fetching file list: ${apiResponse.status} ${apiResponse.statusText}`, { status: 502 });
		}

		const filesData = await apiResponse.json() as Array<{ path: string, type: string, size?: number }>;
		const filePaths = filesData.filter(file =>
			file.type === 'file' &&
			!file.path.startsWith('.git') && 
			!file.path.endsWith('.DS_Store')
		).map(file => file.path);

		console.log("Files to be included in the ZIP:", filePaths);
		if (filePaths.length === 0) {
			return new Response(`No processable files found in ${repo} at revision ${rev}.`, { status: 404 });
		}

		const centralDirectoryEntries: any[] = [];
		let currentGlobalOffset = 0;
		const combinedZipR2Key = `${repo}/${rev}/archive.zip`;

		const mpu = await env.BUCKET.createMultipartUpload(combinedZipR2Key);
		const uploadedParts: R2UploadedPart[] = [];

		let r2PartIndex = 1;
		const R2_PART_SIZE_LIMIT = 60 * 1024 * 1024;
		let r2UploadBuffer: Uint8Array[] = [];
		let r2UploadBufferSize = 0;

		// State for Torrent Creation
		let pieceHashes: Uint8Array[] = [];
		let torrentPieceBufferArrays: Uint8Array[] = [];
		let torrentPieceBufferCurrentSize = 0;
		let totalZipSizeForTorrent = 0;

		async function feedDataToTorrentHasher(data: Uint8Array) {
			if (data.byteLength === 0) return;

			torrentPieceBufferArrays.push(data);
			torrentPieceBufferCurrentSize += data.length;
			totalZipSizeForTorrent += data.length;

			while (torrentPieceBufferCurrentSize >= TORRENT_PIECE_LENGTH) {
				const consolidatedBuffer = concat(...torrentPieceBufferArrays);
				const pieceData = consolidatedBuffer.slice(0, TORRENT_PIECE_LENGTH);
				const remainder = consolidatedBuffer.slice(TORRENT_PIECE_LENGTH);

				const hash = await sha1(pieceData); // sha1 helper is already defined
				pieceHashes.push(hash);

				torrentPieceBufferArrays = remainder.byteLength > 0 ? [remainder] : [];
				torrentPieceBufferCurrentSize = remainder.byteLength;
			}
		}

		async function addDataToR2Stream(data: Uint8Array) {
			if (data.byteLength === 0) return; // Do not process empty chunks

			r2UploadBuffer.push(data);
			r2UploadBufferSize += data.length;
			currentGlobalOffset += data.length;

			while (r2UploadBufferSize >= R2_PART_SIZE_LIMIT) {
				const combined = concat(...r2UploadBuffer);
				const chunkToUpload = combined.slice(0, R2_PART_SIZE_LIMIT);
				const remainder = combined.slice(R2_PART_SIZE_LIMIT);

				console.log(`Uploading R2 part ${r2PartIndex} (from addDataToR2Stream) with size ${chunkToUpload.byteLength}`);
				try {
					uploadedParts.push(await mpu.uploadPart(r2PartIndex++, chunkToUpload));
				} catch (e: any) {
					console.error(`Error in addDataToR2Stream uploading part ${r2PartIndex - 1}:`, e.message, e.stack);
					await mpu.abort(); throw e;
				}

				r2UploadBuffer = remainder.byteLength > 0 ? [remainder] : [];
				r2UploadBufferSize = remainder.byteLength;
			}
		}

		async function flushRemainingR2Stream(): Promise<Uint8Array> {
			if (r2UploadBufferSize > 0) {
				const finalChunk = concat(...r2UploadBuffer);
				r2UploadBuffer = [];
				r2UploadBufferSize = 0;
				return finalChunk;
			}
			return new Uint8Array(0);
		}

		try {
			for (const path of filePaths) {
				console.log(`\n--- Processing file for ZIP: ${path} ---`);
				const filenameForZip = path.split("/").pop()!;
				const localHeaderFileOffset = currentGlobalOffset;

				const localFileHeaderBytes = makeLocalHeader(filenameForZip);
				await addDataToR2Stream(localFileHeaderBytes);
				await feedDataToTorrentHasher(localFileHeaderBytes);

				let currentFileCrc = 0;
				let currentFileRawLen = 0;

				const hfSrc = `https://huggingface.co/${repo}/resolve/${rev}/${path}`;
				const upstream = await fetch(hfSrc, { cf: { cacheEverything: false } });

				if (!upstream.ok || !upstream.body) {
					console.warn(`Skipping file ${path}: Failed to fetch or no body (${upstream.status})`);
					continue;
				}
				console.log(`Streaming content for ${path}...`);
				for await (const chunk of upstream.body) {
					currentFileCrc = crc32(chunk, currentFileCrc);
					currentFileRawLen += chunk.length;
					await addDataToR2Stream(chunk);
					await feedDataToTorrentHasher(chunk);
				}

				const dataDescriptorBytes = makeDataDescriptor(currentFileCrc, currentFileRawLen);
				await addDataToR2Stream(dataDescriptorBytes);
				await feedDataToTorrentHasher(dataDescriptorBytes);

				centralDirectoryEntries.push({
					filename: filenameForZip, crc: currentFileCrc,
					uncompressedSize: currentFileRawLen, compressedSize: currentFileRawLen,
					localHeaderOffset: localHeaderFileOffset
				});
				console.log(`Finished processing ${path}. Size: ${currentFileRawLen}, CRC: ${currentFileCrc}`);
			}

			const remainingDataFromFiles = await flushRemainingR2Stream();

			console.log("\n--- Constructing ZIP Tail (Central Directory & EOCD) ---");
			const startOfCentralDirectoryOffset = currentGlobalOffset;
			const centralDirectoryByteArrays: Uint8Array[] = [];
			for (const entry of centralDirectoryEntries) {
				centralDirectoryByteArrays.push(makeCentralDirectoryEntryBytes(entry.filename, entry.crc, entry.compressedSize, entry.uncompressedSize, entry.localHeaderOffset, ""));
			}
			const fullCentralDirectory = concat(...centralDirectoryByteArrays);
			const eocdBytes = makeEOCDBytes(fullCentralDirectory.length, startOfCentralDirectoryOffset, centralDirectoryEntries.length, "");

			const zipTail = concat(fullCentralDirectory, eocdBytes);
			console.log(`Total Central Directory size: ${fullCentralDirectory.length}, EOCD size: ${eocdBytes.length}, ZIP Tail size: ${zipTail.length}`);

			await feedDataToTorrentHasher(zipTail); // Feed zipTail to torrent hasher

			const finalPartData = concat(remainingDataFromFiles, zipTail);

			if (finalPartData.byteLength > 0) {
				console.log(`Uploading final R2 part ${r2PartIndex} (Combined remaining file data + ZIP Tail) with size ${finalPartData.byteLength}`);
				uploadedParts.push(await mpu.uploadPart(r2PartIndex++, finalPartData));
			} else if (uploadedParts.length === 0) {
				console.warn("Final part data is zero bytes and no other parts were uploaded. Aborting MPU.");
				await mpu.abort();
				return new Response("No data to upload; archive would be empty.", { status: 400 });
			} else {
				console.warn("Final part data (zipTail + remaining) is zero bytes, but other parts were uploaded. This is unusual.");
			}

			console.log("\n--- Finalizing ZIP: Completing Multipart Upload ---");
			console.log(`Total parts to complete: ${uploadedParts.length}`);
			console.log("Parts for MPU complete:", JSON.stringify(uploadedParts.map(p => ({ partNumber: p.partNumber, etag: p.etag })), null, 2));

			await mpu.complete(uploadedParts);

			const finalArchiveUrl = `R2 object: ${combinedZipR2Key}`;
			console.log(`Successfully created ZIP archive: ${finalArchiveUrl}`);

			// --- Torrent File Creation and Upload ---
			console.log("\n--- Creating and Uploading Torrent File ---");

			// Hash any remaining data in the torrent piece buffer
			if (torrentPieceBufferCurrentSize > 0) {
				const lastPieceData = concat(...torrentPieceBufferArrays);
				if (lastPieceData.byteLength > 0) {
					const hash = await sha1(lastPieceData);
					pieceHashes.push(hash);
				}
			}

			const torrentInfoName = combinedZipR2Key.split('/').pop()!;
			const torrentInfoDict = {
				'name': torrentInfoName,
				'piece length': TORRENT_PIECE_LENGTH,
				'pieces': concat(...pieceHashes), // Produces a single Uint8Array
				'length': totalZipSizeForTorrent
			};

			const torrentDataToEncode = {
				'announce': ANNOUNCE_URL,
				'info': torrentInfoDict
			};

			console.log("Torrent meta info (info.pieces omitted for brevity):", JSON.stringify({
				announce: ANNOUNCE_URL,
				info: {
					name: torrentInfoDict.name,
					'piece length': torrentInfoDict['piece length'],
					length: torrentInfoDict.length,
					piecesCount: pieceHashes.length
				}
			}, null, 2));

			const bencodedTorrent = bencode.encode(torrentDataToEncode);
			const torrentR2Key = `${combinedZipR2Key}.torrent`;

			await env.BUCKET.put(torrentR2Key, bencodedTorrent, {
				httpMetadata: { contentType: 'application/x-bittorrent' }
			});
			console.log(`Successfully created and uploaded torrent file: ${torrentR2Key}`);
			// --- End of Torrent File Creation ---

			return new Response(
				`Successfully created ZIP archive: ${combinedZipR2Key}\n` +
				`${centralDirectoryEntries.length} files included.\n` +
				`Total archive size: ${totalZipSizeForTorrent} bytes.\n` + // Use totalZipSizeForTorrent
				`Torrent file created: ${torrentR2Key}`,
				{ headers: { "Content-Type": "text/plain" } }
			);
		} catch (error: any) {
			console.error("Worker fetch handler error:", error.message, error.stack);
			// Ensure MPU is aborted if an error occurred anywhere before completion
			if (mpu && !mpu.complete) { // Check if mpu exists and not already completed/aborted
				console.log("Attempting to abort MPU due to error...");
				try {
					await mpu.abort();
					console.log("MPU aborted successfully.");
				} catch (abortError: any) {
					console.error("Error aborting MPU:", abortError.message, abortError.stack);
				}
			}
			return new Response(`Error processing request: ${error.message}`, { status: 500 });
		}
	}
};

// Helper Functions (ensure all are present and correct)
function makeLocalHeader(filename: string): Uint8Array {
	const utf8Name = new TextEncoder().encode(filename);
	const header = new Uint8Array(30 + utf8Name.length);
	const view = new DataView(header.buffer);
	view.setUint32(0, 0x04034b50, true);  // Local file header signature
	view.setUint16(4, 20, true);     // Version needed to extract (2.0)
	view.setUint16(6, 0x0008, true);     // General purpose bit flag (bit 3: data descriptor)
	view.setUint16(8, 0, true);          // Compression method (0 = store)
	view.setUint16(12, 0, true); 		 // Last mod file time
	view.setUint16(14, 0, true);		 // Last mod file date
	view.setUint32(16, 0, true);         // CRC-32 (placeholder)
	view.setUint32(20, 0, true);        // Compressed size (placeholder)
	view.setUint32(24, 0, true);        // Uncompressed size (placeholder)
	view.setUint16(26, utf8Name.length, true); 
	view.setUint16(28, 0, true);         // Extra field length
	header.set(utf8Name, 30);
	return header;
}

function makeDataDescriptor(crc: number, size: number): Uint8Array {
	const desc = new Uint8Array(12); // CRC + CompSize + UncompSize (NO optional signature)
	const view = new DataView(desc.buffer);
	view.setUint32(0, crc, true);          // CRC-32 (at offset 0)
	view.setUint32(4, size, true);         // Compressed size (at offset 4)
	view.setUint32(8, size, true);        // Uncompressed size (at offset 8)
	return desc;
}

function makeCentralDirectoryEntryBytes(filename: string, crc: number, compressedSize: number, uncompressedSize: number, localHeaderOffset: number, fileComment: string): Uint8Array {
	const utf8Filename = new TextEncoder().encode(filename);
	const utf8FileComment = new TextEncoder().encode(fileComment);
	const entry = new Uint8Array(46 + utf8Filename.length + utf8FileComment.length);
	const view = new DataView(entry.buffer);
	view.setUint32(0, 0x02014b50, true);  // Central directory file header signature
	view.setUint16(4, 20, true);     // Version made by (e.g., 2.0 - PKZip)
	view.setUint16(6, 20, true);     // Version needed to extract (2.0 for data descriptor)
	view.setUint16(8, 0x0008, true);     // General purpose bit flag (bit 3: data descriptor)
	view.setUint16(10, 0, true);         // Compression method (0 = store)
	view.setUint16(12, 0, true);
	view.setUint16(14, 0, true);
	view.setUint32(16, crc, true);
	view.setUint32(20, compressedSize, true);
	view.setUint32(24, uncompressedSize, true);
	view.setUint16(28, utf8Filename.length, true);
	view.setUint16(30, 0, true);         // Extra field length
	view.setUint16(32, utf8FileComment.length, true);
	view.setUint16(34, 0, true);         // Disk number start
	view.setUint16(36, 0, true);         // Internal file attributes
	view.setUint32(38, 0, true);        // External file attributes
	view.setUint32(42, localHeaderOffset, true);
	entry.set(utf8Filename, 46);
	entry.set(utf8FileComment, 46 + utf8Filename.length);
	return entry;
}

function makeEOCDBytes(cdSize: number, cdOffset: number, numEntries: number, zipComment: string): Uint8Array {
	const utf8ZipComment = new TextEncoder().encode(zipComment);
	const eocd = new Uint8Array(22 + utf8ZipComment.length);
	const view = new DataView(eocd.buffer);
	view.setUint32(0, 0x06054b50, true);  // End of central directory signature
	view.setUint16(4, 0, true);          // Number of this disk
	view.setUint16(6, 0, true);          // Disk where central directory starts
	view.setUint16(8, numEntries, true);
	view.setUint16(10, numEntries, true);
	view.setUint32(12, cdSize, true);
	view.setUint32(16, cdOffset, true);
	view.setUint16(20, utf8ZipComment.length, true);
	eocd.set(utf8ZipComment, 22);
	return eocd;
}

function concat(...arrays: Uint8Array[]): Uint8Array {
	const totalLength = arrays.reduce((sum, arr) => sum + arr.length, 0);
	const result = new Uint8Array(totalLength);
	let offset = 0;
	for (const arr of arrays) {
		result.set(arr, offset);
		offset += arr.length;
	}
	return result;
}

function crc32(data: Uint8Array, crc: number = 0): number {
	const table = new Int32Array(256);
	for (let i = 0; i < 256; i++) {
		let c = i;
		for (let j = 0; j < 8; j++) {
			c = (c & 1) ? (0xEDB88320 ^ (c >>> 1)) : (c >>> 1);
		}
		table[i] = c;
	}
	crc = crc ^ (-1);
	for (let i = 0; i < data.length; i++) {
		crc = (crc >>> 8) ^ table[(crc ^ data[i]) & 0xFF];
	}
	return (crc ^ (-1)) >>> 0;
}

async function sha1(data: Uint8Array): Promise<Uint8Array> { // For future torrenting
	const hash = await crypto.subtle.digest('SHA-1', data);
	return new Uint8Array(hash);
}
