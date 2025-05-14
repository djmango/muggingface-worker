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

export default {
	async fetch(req: Request, env: Env): Promise<Response> {
		const url = new URL(req.url);
		console.log("Received req.url:", req.url);

		const repo = url.searchParams.get("repo");
		console.log("Parsed repo parameter:", repo);
		console.log("All search parameters:", url.searchParams.toString());

		const rev = url.searchParams.get("rev") ?? "main";
		// const path = url.searchParams.get("path") ?? "config.json"; // Path will now come from API list

		if (!repo) return new Response("missing ?repo=", { status: 400 });

		// 1. Fetch file list from Hugging Face API
		const hfApiUrl = `https://huggingface.co/api/models/${repo}/tree/${rev}`;
		console.log("Constructed HuggingFace API URL for file list:", hfApiUrl);

		const apiResponse = await fetch(hfApiUrl);
		console.log("HuggingFace API response status:", apiResponse.status, apiResponse.statusText);

		if (!apiResponse.ok) {
			return new Response(`HF API error: ${apiResponse.status} ${apiResponse.statusText}`, { status: 502 });
		}

		const filesData = await apiResponse.json() as Array<{ path: string, type: string, size?: number }>;
		// Filter out .git folder files, .gitattributes, and other common non-content files
		const filePaths = filesData.filter(file =>
			file.type === 'file' &&
			!file.path.startsWith('.git') &&
			!file.path.endsWith('.DS_Store')
		).map(file => file.path);

		console.log("Files to be included in the ZIP:", filePaths);

		if (filePaths.length === 0) {
			return new Response(`No processable files found in ${repo} at revision ${rev}.`, { status: 404 });
		}

		const centralDirectoryEntries: any[] = []; // To store { filename, crc, uncompressedSize, compressedSize, localHeaderOffset }
		let currentGlobalOffset = 0;
		const combinedZipR2Key = `${repo}/${rev}/archive.zip`;

		const mpu = await env.BUCKET.createMultipartUpload(combinedZipR2Key);
		const uploadedParts: R2UploadedPart[] = [];
		let activeR2PartBuffer: Uint8Array[] = [];
		let activeR2PartSize = 0;
		let r2PartIndex = 1;
		const R2_PART_SIZE_LIMIT = 60 * 1024 * 1024; // Slightly less than 64MB for safety

		async function streamChunkToR2(dataChunk: Uint8Array) {
			activeR2PartBuffer.push(dataChunk);
			activeR2PartSize += dataChunk.length;
			currentGlobalOffset += dataChunk.length;

			while (activeR2PartSize >= R2_PART_SIZE_LIMIT && activeR2PartBuffer.length > 0) {
				const bufferToUpload = concat(...activeR2PartBuffer);
				let chunkToUpload: Uint8Array;

				if (bufferToUpload.length > R2_PART_SIZE_LIMIT) {
					chunkToUpload = bufferToUpload.slice(0, R2_PART_SIZE_LIMIT);
					activeR2PartBuffer = [bufferToUpload.slice(R2_PART_SIZE_LIMIT)];
					activeR2PartSize = activeR2PartBuffer[0].length;
				} else {
					chunkToUpload = bufferToUpload;
					activeR2PartBuffer = [];
					activeR2PartSize = 0;
				}
				console.log(`Uploading R2 part ${r2PartIndex} with size ${chunkToUpload.byteLength}`);
				try {
					uploadedParts.push(await mpu.uploadPart(r2PartIndex++, chunkToUpload));
				} catch (e: any) {
					console.error(`Error uploading R2 part ${r2PartIndex - 1}:`, e.message, e.stack);
					// Decide on error handling: rethrow, or try to mark MPU for abort later?
					throw e; // Rethrow for now
				}
			}
		}

		for (const path of filePaths) {
			console.log(`\n--- Adding file to ZIP stream: ${path} ---`);
			const filenameForZip = path.split("/").pop()!;
			const localHeaderFileOffset = currentGlobalOffset;

			const localFileHeaderBytes = makeLocalHeader(filenameForZip);
			await streamChunkToR2(localFileHeaderBytes);

			let currentFileCrc = 0;
			let currentFileRawLen = 0;

			const hfSrc = `https://huggingface.co/${repo}/resolve/${rev}/${path}`;
			const upstream = await fetch(hfSrc, { cf: { cacheEverything: false } });

			if (!upstream.ok || !upstream.body) {
				console.error(`Failed to fetch or get body for ${path}: ${upstream.status}`);
				// Skip this file or abort? For now, log and skip.
				continue;
			}
			console.log(`Streaming content for ${path}...`);
			for await (const chunk of upstream.body) {
				currentFileCrc = crc32(chunk, currentFileCrc);
				currentFileRawLen += chunk.length;
				await streamChunkToR2(chunk);
			}

			const dataDescriptorBytes = makeDataDescriptor(currentFileCrc, currentFileRawLen);
			await streamChunkToR2(dataDescriptorBytes);

			centralDirectoryEntries.push({
				filename: filenameForZip,
				crc: currentFileCrc,
				uncompressedSize: currentFileRawLen,
				compressedSize: currentFileRawLen, // Assuming store-only (no compression)
				localHeaderOffset: localHeaderFileOffset
			});
			console.log(`Finished adding ${path} to ZIP stream. Size: ${currentFileRawLen}, CRC: ${currentFileCrc}`);
		}

		// After all files, if there's remaining data in the buffer, upload it as the last part for file data section.
		if (activeR2PartSize > 0) {
			const finalDataChunk = concat(...activeR2PartBuffer);
			console.log(`Uploading final R2 part ${r2PartIndex} with size ${finalDataChunk.byteLength}`);
			try {
				uploadedParts.push(await mpu.uploadPart(r2PartIndex++, finalDataChunk));
			} catch (e: any) {
				console.error(`Error uploading final R2 part ${r2PartIndex - 1}:`, e.message, e.stack);
				throw e;
			}
		}

		console.log("\n--- All file data streamed to R2 parts --- Succeeded Parts:", uploadedParts.length);
		console.log("Collected Central Directory Entries:", JSON.stringify(centralDirectoryEntries, null, 2));
		console.log("Current Global Offset (expected start of Central Directory):", currentGlobalOffset);

		// --- DEFERRED: Construct Central Directory, EOCD, upload them, complete MPU, create torrent ---
		// For now, we will abort the MPU to avoid incomplete uploads during this dev phase.
		if (mpu) {
			console.log("Aborting MPU as Central Directory generation is not yet implemented.");
			await mpu.abort();
		}

		return new Response(
			`All file data parts streamed. Central Directory construction and MPU completion deferred.\n` +
			`Collected ${centralDirectoryEntries.length} entries for Central Directory.\n` +
			`Total bytes streamed (excluding future CD/EOCD): ${currentGlobalOffset}`,
			{ headers: { "Content-Type": "text/plain" } }
		);
	}
};

// Helper functions
function makeLocalHeader(filename: string): Uint8Array {
	const utf8Name = new TextEncoder().encode(filename);
	const header = new Uint8Array(30 + utf8Name.length);
	const view = new DataView(header.buffer);

	// Local file header signature
	view.setUint32(0, 0x04034b50, true);
	// Version needed to extract
	view.setUint16(4, 0x0014, true);
	// General purpose bit flag (bit 3 = data descriptor)
	view.setUint16(6, 0x0008, true);
	// Compression method (0 = store)
	view.setUint16(8, 0, true);
	// File name length
	view.setUint16(26, utf8Name.length, true);
	// Extra field length
	view.setUint16(28, 0, true);

	header.set(utf8Name, 30);
	return header;
}

function makeDataDescriptor(crc: number, size: number): Uint8Array {
	const desc = new Uint8Array(16);
	const view = new DataView(desc.buffer);

	// Data descriptor signature
	view.setUint32(0, 0x08074b50, true);
	// CRC-32
	view.setUint32(4, crc, true);
	// Compressed size
	view.setUint32(8, size, true);
	// Uncompressed size
	view.setUint32(12, size, true);

	return desc;
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
