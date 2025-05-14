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
		const path = url.searchParams.get("path") ?? "config.json";

		if (!repo) return new Response("missing ?repo=", { status: 400 });

		const hfSrc = `https://huggingface.co/${repo}/resolve/${rev}/${path}`;
		console.log("Constructed HuggingFace URL (hfSrc):", hfSrc);

		// Fetch from HuggingFace
		const upstream = await fetch(hfSrc, { cf: { cacheEverything: false } });
		console.log("HuggingFace response status:", upstream.status, upstream.statusText);

		if (!upstream.ok) return new Response(`HF error: ${upstream.status} ${upstream.statusText}`, { status: 502 });

		const fname = path.split("/").pop()!;
		const zipKey = `${repo}/${rev}/${fname}.zip`;
		const pieceSize = 16 * 1024 * 1024; // 16 MiB pieces

		// Tracking variables
		const pieces: Uint8Array[] = [];
		let pieceBuf = new Uint8Array(0);
		let crc = 0;
		let rawLen = 0;
		let zipOffsetAfterLocalHeaderAndData = 0; // Tracks offset for central directory start

		// Start multipart upload
		const mpu = await env.BUCKET.createMultipartUpload(zipKey);
		const uploadedParts: R2UploadedPart[] = []; // Collect R2UploadedPart objects

		// ZIP local header with data descriptor flag
		const localHeader = makeLocalHeader(fname);
		uploadedParts.push(await mpu.uploadPart(1, localHeader));
		zipOffsetAfterLocalHeaderAndData += localHeader.length;

		// Stream processing
		let partIdx = 2;
		let partSize = 0;
		let partBuf: Uint8Array[] = [];

		for await (const chunk of upstream.body!) {
			// Update CRC32 of uncompressed data
			crc = crc32(chunk, crc);
			rawLen += chunk.length;

			// Accumulate for piece hashing
			pieceBuf = concat(pieceBuf, chunk);
			while (pieceBuf.length >= pieceSize) {
				const piece = pieceBuf.slice(0, pieceSize);
				pieces.push(await sha1(piece));
				pieceBuf = pieceBuf.slice(pieceSize);
			}

			// Add to multipart buffer
			partBuf.push(chunk);
			partSize += chunk.length;
			// zipOffsetAfterLocalHeaderAndData is updated *after* all file data is processed

			// Upload part when buffer is large enough
			if (partSize >= 64 * 1024 * 1024) {
				const currentPartData = concat(...partBuf);
				uploadedParts.push(await mpu.uploadPart(partIdx++, currentPartData));
				partBuf = [];
				partSize = 0;
			}
		}
		zipOffsetAfterLocalHeaderAndData += rawLen; // Now add total rawLen

		// Handle remaining data
		if (pieceBuf.length) {
			pieces.push(await sha1(pieceBuf));
		}
		if (partSize > 0) {
			const finalPartData = concat(...partBuf);
			uploadedParts.push(await mpu.uploadPart(partIdx++, finalPartData));
		}

		// Data descriptor
		const dataDesc = makeDataDescriptor(crc, rawLen);
		uploadedParts.push(await mpu.uploadPart(partIdx++, dataDesc));
		const offsetStartOfCentralDirectory = zipOffsetAfterLocalHeaderAndData + dataDesc.length;

		// Central directory and EOCD
		const centralDir = makeCentralDirectory(fname, crc, rawLen, 0); // Offset of local header is 0
		const eocd = makeEOCD(centralDir.length, offsetStartOfCentralDirectory, 1);
		uploadedParts.push(await mpu.uploadPart(partIdx++, concat(centralDir, eocd)));

		// Complete multipart upload
		await mpu.complete(uploadedParts); // Pass collected parts

		// Create and store torrent
		const finalZipLength = offsetStartOfCentralDirectory + centralDir.length + eocd.length;
		const info = {
			"name": `${fname}.zip`,
			"length": finalZipLength,
			"piece length": pieceSize,
			"pieces": Buffer.concat(pieces) // Assumes Buffer is available via nodejs_compat
		};

		const torrent = bencode.encode(info);

		await env.BUCKET.put(`${zipKey}.torrent`, torrent);

		return new Response(`Success: ${zipKey}
Pieces: ${pieces.length}`);
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

function makeCentralDirectory(filename: string, crc: number, size: number, offsetLocalHeader: number): Uint8Array { // Added offsetLocalHeader
	const utf8Name = new TextEncoder().encode(filename);
	const central = new Uint8Array(46 + utf8Name.length);
	const view = new DataView(central.buffer);

	// Central directory signature
	view.setUint32(0, 0x02014b50, true);
	// Version made by
	view.setUint16(4, 0x031e, true); // Using a common value, like 0x031e for PKZip 2.0 made by MS-DOS
	// Version needed to extract
	view.setUint16(6, 0x0014, true); // 2.0
	// General purpose bit flag
	view.setUint16(8, 0x0008, true); // Bit 3 for data descriptor
	// Compression method
	view.setUint16(10, 0, true); // 0 = store
	// Last mod file time & date (can be zeroed)
	view.setUint16(12, 0, true);
	view.setUint16(14, 0, true);
	// CRC-32
	view.setUint32(16, crc, true);
	// Compressed size
	view.setUint32(20, size, true);
	// Uncompressed size
	view.setUint32(24, size, true);
	// File name length
	view.setUint16(28, utf8Name.length, true);
	// Extra field length
	view.setUint16(30, 0, true);
	// File comment length
	view.setUint16(32, 0, true);
	// Disk number start
	view.setUint16(34, 0, true);
	// Internal file attributes
	view.setUint16(36, 0, true);
	// External file attributes
	view.setUint32(38, 0, true);
	// Relative offset of local header
	view.setUint32(42, offsetLocalHeader, true);

	central.set(utf8Name, 46);
	return central;
}

function makeEOCD(centralDirSize: number, centralDirOffset: number, numEntries: number): Uint8Array {
	const eocd = new Uint8Array(22);
	const view = new DataView(eocd.buffer);

	// End of central directory signature
	view.setUint32(0, 0x06054b50, true);
	// Number of this disk
	view.setUint16(4, 0, true);
	// Disk where central directory starts
	view.setUint16(6, 0, true);
	// Number of entries in central directory on this disk
	view.setUint16(8, numEntries, true);
	// Total number of entries in central directory
	view.setUint16(10, numEntries, true);
	// Size of central directory
	view.setUint32(12, centralDirSize, true);
	// Offset of start of central directory, relative to start of archive
	view.setUint32(16, centralDirOffset, true);
	// ZIP file comment length
	view.setUint16(20, 0, true);

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

async function sha1(data: Uint8Array): Promise<Uint8Array> {
	const hash = await crypto.subtle.digest('SHA-1', data);
	return new Uint8Array(hash);
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
