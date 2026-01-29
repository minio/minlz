// ============================================================================
// MinLZ Block Decoder (port from reference/decoder.go)
// ============================================================================

const MAX_BLOCK_SIZE = 8 << 20; // 8 MiB
const MAX_VIS_SIZE = 256 << 10; // 256 KiB - limit for visualization
const MAX_OPS_DISPLAY = 25000; // Max operations to display in table

class DecodeError extends Error {
    constructor(message) {
        super(message);
        this.name = 'DecodeError';
    }
}

function decodeBlock(src) {
    if (src.length === 0) {
        throw new DecodeError('src length is zero');
    }

    if (src[0] !== 0) {
        if (src[0] === 0xff && src.length >= 10) {
            // Check for .mz stream header: 0xff followed by "MinLz"
            const header = String.fromCharCode(...src.slice(4, 9));
            if (header === 'MinLz') {
                throw new DecodeError('This is a .mz stream file, not a raw block (.mzb). Extract a block first.');
            }
        }
        throw new DecodeError('First byte is not 0 (not a MinLZ block). Expected .mzb file.');
    }

    // Empty block
    if (src.length === 1 && src[0] === 0) {
        return { output: new Uint8Array(0), operations: [] };
    }

    let pos = 1; // Skip first byte
    const operations = [];

    // Read uvarint for expected size
    let wantSize = 0;
    for (let i = 0; i < 70; i += 7) {
        if (pos >= src.length) {
            throw new DecodeError('unable to read length');
        }
        const v = src[pos++];
        wantSize |= (v & 0x7f) << i;
        if (wantSize > MAX_BLOCK_SIZE) {
            throw new DecodeError('invalid destination size');
        }
        if ((v & 0x80) === 0) break;
    }

    if (wantSize < 0 || wantSize > MAX_BLOCK_SIZE) {
        throw new DecodeError(`invalid destination size ${wantSize}`);
    }

    // If size is 0, rest is literals
    if (wantSize === 0) {
        const literals = src.slice(pos);
        operations.push({
            type: 'literal',
            srcInputOffset: pos,
            encodedBytes: src.slice(0, pos),
            dstOffset: 0,
            length: literals.length,
            literals: literals
        });
        return { output: literals, operations };
    }

    if (wantSize < src.length - pos) {
        throw new DecodeError('decompressed smaller than compressed size');
    }

    const dst = new Uint8Array(wantSize);
    let dstPos = 0;
    let offset = 1; // Repeat offset, starts at 1

    // Helper functions
    const readOne = () => {
        if (pos < src.length) return src[pos++];
        return null;
    };

    const readTwo = () => {
        if (pos + 1 < src.length) {
            const v = src[pos] | (src[pos + 1] << 8);
            pos += 2;
            return v;
        }
        return null;
    };

    const readThree = () => {
        if (pos + 2 < src.length) {
            const v = src[pos] | (src[pos + 1] << 8) | (src[pos + 2] << 16);
            pos += 3;
            return v;
        }
        return null;
    };

    const readN = (n) => {
        if (pos + n <= src.length) {
            const data = src.slice(pos, pos + n);
            pos += n;
            return data;
        }
        return null;
    };

    while (pos < src.length) {
        const opStartPos = pos;
        const tag = readOne();
        if (tag === null) break;

        const tagType = tag & 3;
        let value = tag >> 2;
        let length = 0;
        let opType = '';
        let copyOffset = null;
        let copySourceStart = null;
        let literals = null;
        let fusedLiterals = null;

        switch (tagType) {
            case 0: { // Literal or Repeat
                const isRepeat = (value & 1) !== 0;
                value = value >> 1;

                // Decode length
                if (value < 29) {
                    length = value + 1;
                } else if (value === 29) {
                    const ext = readOne();
                    if (ext === null) throw new DecodeError(`lit tag 29: unable to read length at dst pos ${dstPos}`);
                    length = 30 + ext;
                } else if (value === 30) {
                    const ext = readTwo();
                    if (ext === null) throw new DecodeError(`lit tag 30: unable to read length at dst pos ${dstPos}`);
                    length = 30 + ext;
                } else if (value === 31) {
                    const ext = readThree();
                    if (ext === null) throw new DecodeError(`lit tag 31: unable to read length at dst pos ${dstPos}`);
                    length = 30 + ext;
                }

                if (isRepeat) {
                    opType = 'repeat';
                    copyOffset = offset;
                    copySourceStart = dstPos - offset;
                    // Fall through to copy execution below
                } else {
                    opType = 'literal';
                    literals = readN(length);
                    if (literals === null) throw new DecodeError(`literal length ${length} exceed source at dst pos ${dstPos}`);

                    if (dstPos + length > wantSize) {
                        throw new DecodeError(`literal length ${length} exceed destination at dst pos ${dstPos}`);
                    }

                    dst.set(literals, dstPos);

                    operations.push({
                        type: 'literal',
                        srcInputOffset: opStartPos,
                        encodedBytes: src.slice(opStartPos, pos),
                        dstOffset: dstPos,
                        length: length,
                        literals: literals
                    });

                    dstPos += length;
                    continue;
                }
                break;
            }

            case 1: { // Copy1 (10-bit offset)
                opType = 'copy1';
                length = value & 15;
                const offByte = readOne();
                if (offByte === null) throw new DecodeError(`copy 1: unable to read offset at dst pos ${dstPos}`);

                offset = (offByte << 2) | (value >> 4);

                if (length === 15) {
                    const extLen = readOne();
                    if (extLen === null) throw new DecodeError(`copy 1: unable to read length at dst pos ${dstPos}`);
                    length = 18 + extLen;
                } else {
                    length += 4;
                }

                offset += 1; // Minimum offset is 1
                copyOffset = offset;
                copySourceStart = dstPos - offset;
                break;
            }

            case 2: { // Copy2 (16-bit offset)
                opType = 'copy2';
                const off = readTwo();
                if (off === null) throw new DecodeError(`copy 2: unable to read offset at dst pos ${dstPos}`);

                if (value <= 60) {
                    length = value + 4;
                } else if (value === 61) {
                    const ext = readOne();
                    if (ext === null) throw new DecodeError(`copy 2.61: unable to read length at dst pos ${dstPos}`);
                    length = 64 + ext;
                } else if (value === 62) {
                    const ext = readTwo();
                    if (ext === null) throw new DecodeError(`copy 2.62: unable to read length at dst pos ${dstPos}`);
                    length = 64 + ext;
                } else if (value === 63) {
                    const ext = readThree();
                    if (ext === null) throw new DecodeError(`copy 2.63: unable to read length at dst pos ${dstPos}`);
                    length = 64 + ext;
                }

                offset = off + 64; // Minimum offset is 64
                copyOffset = offset;
                copySourceStart = dstPos - offset;
                break;
            }

            case 3: { // Fused Copy2 or Copy3
                const isCopy3 = (value & 1) === 1;
                let litLen = (value >> 1) & 3;

                if (!isCopy3) {
                    // Fused Copy2
                    opType = 'copy2';
                    const off = readTwo();
                    if (off === null) throw new DecodeError(`copy 2, fused: unable to read offset at dst pos ${dstPos}`);

                    length = (value >> 3) + 4;
                    litLen += 1; // Minimum 1 literal for fused copy2
                    offset = off + 64;
                } else {
                    // Copy3
                    opType = 'copy3';
                    const v2 = readThree();
                    if (v2 === null) throw new DecodeError(`copy 3: unable to read value at dst pos ${dstPos}`);

                    value = value | (v2 << 6);
                    offset = (value >> 9) + 65536;
                    const lenVal = (value >> 3) & 63;

                    if (lenVal < 61) {
                        length = lenVal + 4;
                    } else if (lenVal === 61) {
                        const ext = readOne();
                        if (ext === null) throw new DecodeError(`copy 3.61: unable to read length at dst pos ${dstPos}`);
                        length = 64 + ext;
                    } else if (lenVal === 62) {
                        const ext = readTwo();
                        if (ext === null) throw new DecodeError(`copy 3.62: unable to read length at dst pos ${dstPos}`);
                        length = 64 + ext;
                    } else if (lenVal === 63) {
                        const ext = readThree();
                        if (ext === null) throw new DecodeError(`copy 3.63: unable to read length at dst pos ${dstPos}`);
                        length = 64 + ext;
                    }
                }

                // Read fused literals
                if (litLen > 0) {
                    fusedLiterals = readN(litLen);
                    if (fusedLiterals === null) throw new DecodeError(`copy 3: unable to read extra literals at dst pos ${dstPos}`);

                    if (dstPos + litLen > wantSize) {
                        throw new DecodeError(`copy 3: extra literal output size exceeded at dst pos ${dstPos}`);
                    }

                    dst.set(fusedLiterals, dstPos);
                    dstPos += litLen;
                }

                copyOffset = offset;
                copySourceStart = dstPos - offset;
                break;
            }
        }

        // Execute copy (for repeat, copy1, copy2, copy3)
        if (dstPos + length > wantSize) {
            throw new DecodeError(`copy length ${length} exceeds dst size at dst pos ${dstPos}`);
        }
        if (offset > dstPos) {
            throw new DecodeError(`copy offset ${offset} exceeds dst size ${dstPos}`);
        }

        const inPos = dstPos - offset;

        // Record operation before copy
        const opRecord = {
            type: opType,
            srcInputOffset: opStartPos,
            encodedBytes: src.slice(opStartPos, pos),
            dstOffset: fusedLiterals ? dstPos - fusedLiterals.length : dstPos,
            length: length + (fusedLiterals ? fusedLiterals.length : 0),
            copyOffset: copyOffset,
            copySourceStart: copySourceStart,
            copyLength: length
        };

        if (fusedLiterals) {
            opRecord.fusedLiterals = fusedLiterals;
            opRecord.copyDstOffset = dstPos;
        }

        operations.push(opRecord);

        // Do the copy (handles overlapping copies)
        for (let i = 0; i < length; i++) {
            dst[dstPos++] = dst[inPos + i];
        }
    }

    if (dstPos !== wantSize) {
        throw new DecodeError(`mismatching output size, got ${dstPos}, want ${wantSize}`);
    }

    return { output: dst, operations };
}

// ============================================================================
// UI State & Visualization
// ============================================================================

let state = {
    compressed: null,
    output: null,
    visLength: 0,   // Capped at MAX_VIS_SIZE for rendering
    operations: [],
    selectedOp: null,
    opByteMap: null, // Maps each output byte to its operation index
    typeStats: null, // Statistics per operation type

    // Canvas state
    zoom: 1,
    panX: 0,
    panY: 0,
    canvasWidth: 0,
    canvasHeight: 0,
    dataWidth: 0,
    dataHeight: 0,

    // Filter state
    filters: {
        literal: true,
        repeat: true,
        copy1: true,
        copy2: true,
        copy3: true
    }
};

// Color tints for operation types
const OP_COLORS = {
    literal: [138, 138, 138],
    repeat:  [120, 120, 200],
    copy1:   [80, 180, 80],
    copy2:   [180, 180, 100],
    copy3:   [180, 120, 110]
};

function getPixelColor(byteValue, opType) {
    const grayFactor = 0.0;
    const colorFactor = 1.0 - grayFactor;
    const gray = 150 * grayFactor;
    const [r, g, b] = OP_COLORS[opType] || OP_COLORS.literal;
    return [
        Math.round(gray + r * colorFactor),
        Math.round(gray + g * colorFactor),
        Math.round(gray + b * colorFactor)
    ];

}

function formatHex(opBytes, litCount = 0) {
    // Show op encoding bytes, but for literals just show [n] instead of actual bytes
    const headerLen = opBytes.length - litCount;
    const arr = Array.from(opBytes.slice(0, headerLen));
    let hex = arr.map(b => b.toString(16).padStart(2, '0')).join(' ');
    if (litCount > 0) {
        hex += ` [${litCount}]`;
    }
    return hex;
}

function formatSize(bytes) {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
}

function formatAscii(byte) {
    if (byte >= 32 && byte < 127) return String.fromCharCode(byte);
    return '.';
}

// ============================================================================
// DOM Elements
// ============================================================================

const elements = {
    stats: {
        compressed: document.getElementById('stat-compressed'),
        decompressed: document.getElementById('stat-decompressed'),
        ratio: document.getElementById('stat-ratio'),
        ops: document.getElementById('stat-ops')
    },
    opTbody: document.getElementById('op-tbody'),
    tableContainer: document.getElementById('table-container'),
    canvas: document.getElementById('canvas'),
    canvasWrapper: document.getElementById('canvas-wrapper'),
    canvasContainer: document.getElementById('canvas-container'),
    canvasToolbar: document.getElementById('canvas-toolbar'),
    canvasInfo: document.getElementById('canvas-info'),
    emptyState: document.getElementById('empty-state'),
    legend: document.getElementById('legend'),
    dropZone: document.getElementById('drop-zone'),
    tooltip: document.getElementById('tooltip'),
    tipPos: document.getElementById('tip-pos'),
    tipVal: document.getElementById('tip-val'),
    tipOp: document.getElementById('tip-op'),
    zoomLevel: document.getElementById('zoom-level'),
    fileInput: document.getElementById('file-input'),
    filterBtns: document.querySelectorAll('.filter-btn'),
    loadingOverlay: document.getElementById('loading-overlay'),
    loadingText: document.querySelector('.loading-text')
};

// ============================================================================
// Rendering
// ============================================================================

function updateStats() {
    if (!state.compressed) {
        elements.stats.compressed.textContent = '-';
        elements.stats.decompressed.textContent = '-';
        elements.stats.ratio.textContent = '-';
        elements.stats.ops.textContent = '-';
        return;
    }

    elements.stats.compressed.textContent = formatSize(state.compressed.length);

    let decompText = formatSize(state.output.length);
    if (state.visLength < state.output.length) {
        decompText += ` (vis: ${formatSize(state.visLength)})`;
    }
    elements.stats.decompressed.textContent = decompText;

    const ratio = state.output.length / state.compressed.length;
    elements.stats.ratio.textContent = ratio.toFixed(2) + 'x';

    let opsText = state.operations.length.toLocaleString();
    if (state.operations.length > MAX_OPS_DISPLAY) {
        opsText += ` (showing ${MAX_OPS_DISPLAY.toLocaleString()})`;
    }
    elements.stats.ops.textContent = opsText;
}

function renderTable() {
    const tbody = elements.opTbody;
    tbody.innerHTML = '';

    let displayed = 0;
    state.operations.forEach((op, idx) => {
        if (!state.filters[op.type]) return;
        if (displayed >= MAX_OPS_DISPLAY) return;
        displayed++;

        const tr = document.createElement('tr');
        tr.dataset.opIndex = idx;

        if (state.selectedOp === idx) {
            tr.classList.add('selected');
        }

        // Index
        const tdIdx = document.createElement('td');
        tdIdx.textContent = idx;
        tr.appendChild(tdIdx);

        // Type badge
        const tdType = document.createElement('td');
        const badge = document.createElement('span');
        badge.className = `type-badge type-${op.type}`;
        badge.textContent = op.type;
        tdType.appendChild(badge);
        tr.appendChild(tdType);

        // Hex encoding (show literal count as [n] instead of actual bytes)
        const tdHex = document.createElement('td');
        tdHex.className = 'hex';
        const litCount = op.literals ? op.literals.length : (op.fusedLiterals ? op.fusedLiterals.length : 0);
        tdHex.textContent = formatHex(op.encodedBytes, litCount);
        tr.appendChild(tdHex);

        // Details
        const tdDetails = document.createElement('td');
        if (op.type === 'literal') {
            const preview = Array.from(op.literals.slice(0, 8)).map(formatAscii).join('');
            tdDetails.innerHTML = `<span class="length-val">${op.length}</span> bytes: "${preview}${op.length > 8 ? '...' : ''}"`;
        } else if (op.type === 'repeat') {
            tdDetails.innerHTML = `len=<span class="length-val">${op.copyLength}</span> off=<span class="offset-val">${op.copyOffset}</span>`;
        } else {
            let details = '';
            if (op.fusedLiterals) {
                const preview = Array.from(op.fusedLiterals).map(formatAscii).join('');
                details = `lits=<span class="length-val">${op.fusedLiterals.length}</span> "${preview}" `;
            }
            details += `len=<span class="length-val">${op.copyLength}</span> off=<span class="offset-val">${op.copyOffset}</span>`;
            tdDetails.innerHTML = details;
        }
        tr.appendChild(tdDetails);

        // Dst offset
        const tdDst = document.createElement('td');
        tdDst.className = 'offset-val';
        tdDst.textContent = op.dstOffset.toLocaleString();
        tr.appendChild(tdDst);

        tr.addEventListener('click', () => selectOperation(idx));
        tbody.appendChild(tr);
    });
}

function buildOpByteMap() {
    if (!state.output) return;

    state.opByteMap = new Uint32Array(state.visLength);

    for (let i = 0; i < state.operations.length; i++) {
        const op = state.operations[i];
        const start = op.dstOffset;
        const end = Math.min(op.dstOffset + op.length, state.visLength);
        for (let j = start; j < end; j++) {
            state.opByteMap[j] = i;
        }
    }
}

function computeTypeStats() {
    const types = ['literal', 'repeat', 'copy1', 'copy2', 'copy3'];
    const stats = {};
    for (const t of types) {
        stats[t] = { count: 0, outputBytes: 0, encodingBytes: 0, literalBytes: 0, bytesSaved: 0, withLiterals: 0 };
    }

    for (const op of state.operations) {
        const s = stats[op.type];
        s.count++;
        s.outputBytes += op.length;

        const litCount = op.literals ? op.literals.length : (op.fusedLiterals ? op.fusedLiterals.length : 0);
        const encBytes = op.encodedBytes.length - litCount;
        s.encodingBytes += encBytes;
        s.literalBytes += litCount;

        if (op.fusedLiterals && op.fusedLiterals.length > 0) {
            s.withLiterals++;
        }

        if (op.type !== 'literal') {
            const copyLen = op.copyLength || (op.length - litCount);
            s.bytesSaved += copyLen - encBytes;
        }
    }

    state.typeStats = stats;
}

function renderLegendStats() {
    if (!state.typeStats || !state.output) return;

    const totalOps = state.operations.length || 1;
    const totalOut = state.output.length || 1;
    const totalComp = state.compressed.length || 1;

    for (const [type, s] of Object.entries(state.typeStats)) {
        const pctOps = (s.count / totalOps * 100).toFixed(1);
        const pctOut = (s.outputBytes / totalOut * 100).toFixed(1);
        const avgOut = s.count ? (s.outputBytes / s.count).toFixed(2) : '0';
        const avgEnc = s.count ? (s.encodingBytes / s.count).toFixed(2) : '0';
        const avgSaved = s.count ? (s.bytesSaved / s.count).toFixed(2) : '0';

        let html = `<table>
            <tr><td>Count:</td><td>${s.count.toLocaleString()} (${pctOps}%)</td></tr>`;

        if ((type === 'copy2' || type === 'copy3') && s.count > 0) {
            const pctWithLits = (s.withLiterals / s.count * 100).toFixed(1);
            html += `<tr><td>With literals:</td><td>${s.withLiterals.toLocaleString()} (${pctWithLits}%)</td></tr>`;
            if (s.literalBytes > 0) {
                html += `<tr><td>Literals:</td><td>${s.literalBytes.toLocaleString()} B</td></tr>`;
            }
        }

        const pctEnc = (s.encodingBytes / totalComp * 100).toFixed(1);
        html += `<tr><td>Output:</td><td>${s.outputBytes.toLocaleString()} B (${pctOut}%)</td></tr>
            <tr><td>Avg output:</td><td>${avgOut} B</td></tr>
            <tr><td>Encoding:</td><td>${s.encodingBytes.toLocaleString()} B (${pctEnc}%)</td></tr>
            <tr><td>Avg encoding:</td><td>${avgEnc} B</td></tr>`;

        if (type === 'literal') {
            html += `<tr><td>Literals:</td><td>${s.literalBytes.toLocaleString()} B</td></tr>`;
        }

        if (type !== 'literal') {
            html += `<tr><td>Saved:</td><td>${s.bytesSaved.toLocaleString()} B (avg ${avgSaved})</td></tr>`;
        }

        html += '</table>';

        // Update both bottom legend and top filter button stats
        const bottomEl = document.getElementById(`stats-${type}`);
        if (bottomEl) bottomEl.innerHTML = html;

        const topEl = document.getElementById(`stats-top-${type}`);
        if (topEl) topEl.innerHTML = html;
    }
}

function calculateCanvasDimensions() {
    if (!state.output || state.visLength === 0) return;

    const len = state.visLength;
    const maxDim = 4096;

    // Target square: width ≈ sqrt(len), rounded up to multiple of 16 for alignment
    let width = Math.ceil(Math.sqrt(len) / 16) * 16;
    width = Math.max(64, Math.min(width, maxDim));

    const height = Math.min(Math.ceil(len / width), maxDim);

    state.dataWidth = width;
    state.dataHeight = height;
}

function renderCanvas() {
    if (!state.output || state.visLength === 0) return;

    const canvas = elements.canvas;
    const ctx = canvas.getContext('2d');

    canvas.width = state.dataWidth;
    canvas.height = state.dataHeight;

    const imageData = ctx.createImageData(state.dataWidth, state.dataHeight);
    const data = imageData.data;

    for (let i = 0; i < state.visLength; i++) {
        const x = i % state.dataWidth;
        const y = Math.floor(i / state.dataWidth);

        if (y >= state.dataHeight) break;

        const opIdx = state.opByteMap[i];
        const op = state.operations[opIdx];
        const [r, g, b] = getPixelColor(state.output[i], op.type);

        const pixelIdx = (y * state.dataWidth + x) * 4;
        data[pixelIdx] = r;
        data[pixelIdx + 1] = g;
        data[pixelIdx + 2] = b;
        data[pixelIdx + 3] = 255;
    }

    // Fill remaining pixels with dark color
    for (let i = state.visLength; i < state.dataWidth * state.dataHeight; i++) {
        const pixelIdx = i * 4;
        data[pixelIdx] = 20;
        data[pixelIdx + 1] = 20;
        data[pixelIdx + 2] = 30;
        data[pixelIdx + 3] = 255;
    }

    ctx.putImageData(imageData, 0, 0);

    // Draw selection highlight
    if (state.selectedOp !== null) {
        drawSelectionHighlight(ctx);
    }

    updateCanvasTransform();
}

function drawSelectionHighlight(ctx) {
    const op = state.operations[state.selectedOp];
    if (!op) return;

    // Draw source highlight for copies first (red/magenta, underneath)
    if (op.copySourceStart !== undefined && op.copySourceStart >= 0) {
        const copyStart = op.fusedLiterals ? op.copyDstOffset - op.copyOffset : op.copySourceStart;
        ctx.fillStyle = 'rgba(255, 0, 0, 0.7)';
        drawRangeFilled(ctx, copyStart, op.copyLength);
    }

    // Draw destination highlight (cyan/blue, on top)
    ctx.fillStyle = 'rgba(255, 255, 255, 0.75)';
    drawRangeFilled(ctx, op.dstOffset, op.length);
}

function drawRangeFilled(ctx, start, length) {
    if (length === 0 || start < 0) return;

    const startX = start % state.dataWidth;
    const startY = Math.floor(start / state.dataWidth);
    const endPos = start + length - 1;
    const endX = endPos % state.dataWidth;
    const endY = Math.floor(endPos / state.dataWidth);

    ctx.beginPath();

    if (startY === endY) {
        // Single row - simple rect
        ctx.rect(startX, startY, length, 1);
    } else {
        // Multiple rows - create polygon path
        ctx.moveTo(startX, startY);
        ctx.lineTo(state.dataWidth, startY);
        ctx.lineTo(state.dataWidth, endY);
        ctx.lineTo(endX + 1, endY);
        ctx.lineTo(endX + 1, endY + 1);
        ctx.lineTo(0, endY + 1);
        ctx.lineTo(0, startY + 1);
        ctx.lineTo(startX, startY + 1);
        ctx.closePath();
    }

    ctx.fill();
}

function updateCanvasTransform() {
    const canvas = elements.canvas;
    const wrapper = elements.canvasWrapper;

    const wrapperRect = wrapper.getBoundingClientRect();
    state.canvasWidth = wrapperRect.width;
    state.canvasHeight = wrapperRect.height;

    const scaledWidth = state.dataWidth * state.zoom;
    const scaledHeight = state.dataHeight * state.zoom;

    // Center if smaller than viewport
    let x = state.panX;
    let y = state.panY;

    if (scaledWidth < state.canvasWidth) {
        x = (state.canvasWidth - scaledWidth) / 2;
    }
    if (scaledHeight < state.canvasHeight) {
        y = (state.canvasHeight - scaledHeight) / 2;
    }

    canvas.style.width = scaledWidth + 'px';
    canvas.style.height = scaledHeight + 'px';
    canvas.style.left = x + 'px';
    canvas.style.top = y + 'px';

    elements.zoomLevel.textContent = state.zoom + 'x';
}

function selectOperation(idx) {
    state.selectedOp = idx;

    // Update table selection
    const rows = elements.opTbody.querySelectorAll('tr');
    rows.forEach(row => {
        if (parseInt(row.dataset.opIndex) === idx) {
            row.classList.add('selected');
            row.scrollIntoView({ behavior: 'smooth', block: 'center' });
        } else {
            row.classList.remove('selected');
        }
    });

    // Redraw canvas with highlight
    renderCanvas();
}

function zoomTo(level, centerX, centerY) {
    const oldZoom = state.zoom;
    state.zoom = Math.max(1, Math.min(64, level));

    if (centerX !== undefined && centerY !== undefined) {
        // Zoom towards cursor position
        const scale = state.zoom / oldZoom;
        state.panX = centerX - (centerX - state.panX) * scale;
        state.panY = centerY - (centerY - state.panY) * scale;
    }

    updateCanvasTransform();
}

function fitToView() {
    const wrapper = elements.canvasWrapper;
    const rect = wrapper.getBoundingClientRect();

    const scaleX = rect.width / state.dataWidth;
    const scaleY = rect.height / state.dataHeight;

    state.zoom = Math.max(1, Math.floor(Math.min(scaleX, scaleY)));
    state.panX = 0;
    state.panY = 0;

    updateCanvasTransform();
}

// ============================================================================
// File Loading
// ============================================================================

function showLoading(text) {
    elements.loadingText.textContent = text || 'Processing...';
    elements.loadingOverlay.classList.add('active');
}

function hideLoading() {
    elements.loadingOverlay.classList.remove('active');
}

function loadBlock(data) {
    showLoading('Decoding...');

    // Use setTimeout to let the spinner render before heavy processing
    setTimeout(() => {
        try {
            const result = decodeBlock(data);

            state.compressed = data;
            state.output = result.output;
            state.visLength = Math.min(result.output.length, MAX_VIS_SIZE);
            state.operations = result.operations;
            state.selectedOp = null;
            state.zoom = 1;
            state.panX = 0;
            state.panY = 0;

            showLoading('Building visualization...');

            setTimeout(() => {
                try {
                    buildOpByteMap();
                    computeTypeStats();
                    calculateCanvasDimensions();

                    updateStats();
                    renderTable();
                    renderLegendStats();

                    // Show canvas UI
                    elements.emptyState.style.display = 'none';
                    elements.canvasToolbar.style.display = 'flex';
                    elements.canvasWrapper.style.display = 'block';
                    elements.legend.style.display = 'flex';
                    document.getElementById('btn-download').disabled = false;

                    renderCanvas();
                    fitToView();
                    hideLoading();
                } catch (e) {
                    hideLoading();
                    alert('Render error: ' + e.message);
                    console.error(e);
                }
            }, 10);

        } catch (e) {
            hideLoading();
            alert('Decode error: ' + e.message);
            console.error(e);
        }
    }, 10);
}

// Embedded sample: Mark.Twain-Tom.Sawyer.txt.mzb (base64)
const SAMPLE_B64 =  `ANhu6BpQcm9kdWNlZCBieSBEYXZpZCBXaWRnZXIuIFRoZSBwcmV2aW91cyBlZGl0aW9uIHdhcyB1cGRhdEkMcEpvc2UKTWVuZW5kZXouCgUAACCM6ABUSEUgQURWRU5UVVJFUyBPRiBUT00gU0FXWUVSCiDsAQhCWb0IC0hNQVJLIFRXQUlOvQkEyChTYW11ZWwgTGFuZ2hvcm5lIENsZW1lbnMpUm4AEQDoVVAgUiBFIEYgQSBDIEUKCk1PU1Qgb2YgdGhlIGFkdmVudHVyZXMgcmVjb3JkZWQgaW4gdGhpcyBib29rIHJlYWxseSBvY2N1cnJlZDsgb25lIG9yCnR3byB3ZXJlIGV4cGVyaWVuY2VzIG9mIG15IG93biwFF0hyZXN0IHRob3NlQQYwYm95cyB3aIkNSApzY2hvb2xtYXQ06B1pbmUuIEh1Y2sgRmlubiBpcyBkcmF3biBmcm9tIGxpZmU7IFRvbSBTYXd5ZXIgYWxzbywgYnV0Cm5vdMkIgGFuIGluZGl2aWR1YWwtLWhlwRBAYSBjb21iaW5hxXzNPGhjaGFyYWN0ZXJpc3RpY4EfKAp0aHJlZVUoYG0gSSBrbmV3LCBhbmSBDIByZWZvcmUgYmVsb25ncyB0b8kRMG9tcG9zaXTBQxMIAGRlcjhhcmNoaXRlY0FUEzoCLgoKUG9kZCBzdXBlcnN0JIEPOHVjaGVkIHVwwaHBUDNiAmFsbKBhbGVudCBhbW9uZyBjaGlsZHJlbgrBIChzbGF2ZXNJZhMYAWUgVwMbAWELPQFlIEubAG9kaGlzIHN0b3J5LS10aGF0wT+YdG8gc2F5LAp0aGlydHkgb3IgZm8BAph5ZWFycyBhZ28uCgpBbHRob3VnaIFuhXwDKQBpA8oCdDhkIG1haW5seUENRRtIZW50ZXJ0YWlubQEqEXXIYW5kCmdpcmxzLCBJIGhvcGUgaXQgd2lsbCBBZzhiZSBzaHVubgnaMxYBbWVuCHdvQQIDkQBvWGF0IGFjY291bnQsCgEaewsCcGFydGBwbGFuIGhhcyBiZWVuxV0IcnmBAbBwbGVhc2FudGx5IHJlbWluZCBhZHVsdIWSaHdoYXQKdGhleSBvbmNlCVhIdGhlbXNlbHZlc8l0KG9mIGhvd4EFKHkgZmVsdA16gUHNAihhbGtlZCwFXwEUKCBxdWVlcsk/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`

function loadSampleBlock() {
    showLoading('Loading sample...');
    setTimeout(() => {
        try {
            const binary = atob(SAMPLE_B64);
            const bytes = new Uint8Array(binary.length);
            for (let i = 0; i < binary.length; i++) {
                bytes[i] = binary.charCodeAt(i);
            }
            hideLoading();
            loadBlock(bytes);
        } catch (err) {
            hideLoading();
            alert('Error loading sample: ' + err.message);
        }
    }, 10);
}

// ============================================================================
// Event Handlers
// ============================================================================

// File input
document.getElementById('btn-load').addEventListener('click', () => {
    elements.fileInput.click();
});

elements.fileInput.addEventListener('change', (e) => {
    const file = e.target.files[0];
    if (file) {
        const reader = new FileReader();
        reader.onload = () => loadBlock(new Uint8Array(reader.result));
        reader.readAsArrayBuffer(file);
    }
});

document.getElementById('btn-sample').addEventListener('click', loadSampleBlock);

document.getElementById('btn-download').addEventListener('click', () => {
    if (!state.output) return;
    const blob = new Blob([state.output], { type: 'application/octet-stream' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'decoded.bin';
    a.click();
    URL.revokeObjectURL(url);
});

// Drag and drop
document.addEventListener('dragover', (e) => {
    e.preventDefault();
    elements.dropZone.classList.add('active');
});

document.addEventListener('dragleave', (e) => {
    if (e.relatedTarget === null || !document.body.contains(e.relatedTarget)) {
        elements.dropZone.classList.remove('active');
    }
});

document.addEventListener('drop', (e) => {
    e.preventDefault();
    elements.dropZone.classList.remove('active');

    const file = e.dataTransfer.files[0];
    if (file) {
        const reader = new FileReader();
        reader.onload = () => loadBlock(new Uint8Array(reader.result));
        reader.readAsArrayBuffer(file);
    }
});

// Zoom controls
document.getElementById('zoom-in').addEventListener('click', () => zoomTo(state.zoom * 2));
document.getElementById('zoom-out').addEventListener('click', () => zoomTo(state.zoom / 2));
document.getElementById('zoom-fit').addEventListener('click', fitToView);

// Mouse wheel zoom
elements.canvasWrapper.addEventListener('wheel', (e) => {
    e.preventDefault();

    const rect = elements.canvasWrapper.getBoundingClientRect();
    const x = e.clientX - rect.left;
    const y = e.clientY - rect.top;

    const delta = e.deltaY > 0 ? 0.8 : 1.25;
    zoomTo(state.zoom * delta, x, y);
});

// Pan handling
let isPanning = false;
let wasPanning = false;
let lastPanX = 0;
let lastPanY = 0;

elements.canvasWrapper.addEventListener('mousedown', (e) => {
    if (e.button === 0) {
        isPanning = true;
        wasPanning = false;
        lastPanX = e.clientX;
        lastPanY = e.clientY;
        elements.canvasWrapper.style.cursor = 'grabbing';
    }
});

document.addEventListener('mousemove', (e) => {
    if (isPanning) {
        state.panX += e.clientX - lastPanX;
        state.panY += e.clientY - lastPanY;
        lastPanX = e.clientX;
        lastPanY = e.clientY;
        wasPanning = true;
        updateCanvasTransform();
    }
});

document.addEventListener('mouseup', () => {
    isPanning = false;
    elements.canvasWrapper.style.cursor = 'grab';
});

// Canvas click to select operation
elements.canvas.addEventListener('click', (e) => {
    if (!state.output) return;
    if (wasPanning) return; // Don't select after panning

    const rect = elements.canvas.getBoundingClientRect();
    const x = Math.floor((e.clientX - rect.left) / state.zoom);
    const y = Math.floor((e.clientY - rect.top) / state.zoom);

    const bytePos = y * state.dataWidth + x;

    if (bytePos >= 0 && bytePos < state.visLength) {
        const opIdx = state.opByteMap[bytePos];
        selectOperation(opIdx);
    }
});

// Canvas hover tooltip
elements.canvasWrapper.addEventListener('mousemove', (e) => {
    if (!state.output || isPanning) {
        elements.tooltip.classList.remove('visible');
        return;
    }

    const canvasRect = elements.canvas.getBoundingClientRect();
    const x = Math.floor((e.clientX - canvasRect.left) / state.zoom);
    const y = Math.floor((e.clientY - canvasRect.top) / state.zoom);

    const bytePos = y * state.dataWidth + x;

    if (bytePos >= 0 && bytePos < state.visLength) {
        const opIdx = state.opByteMap[bytePos];
        const op = state.operations[opIdx];

        // Show operation details
        elements.tipPos.textContent = `#${opIdx} ${op.type}`;

        if (op.type === 'literal') {
            const preview = Array.from(op.literals.slice(0, 16)).map(formatAscii).join('');
            elements.tipVal.textContent = `${op.length} bytes: "${preview}${op.length > 16 ? '...' : ''}"`;
        } else {
            let details = '';
            if (op.fusedLiterals) {
                const preview = Array.from(op.fusedLiterals).map(formatAscii).join('');
                details = `lits=${op.fusedLiterals.length} "${preview}" `;
            }
            details += `len=${op.copyLength} off=${op.copyOffset}`;
            // Show preview of copied data
            const copyStart = op.fusedLiterals ? op.copyDstOffset - op.copyOffset : op.copySourceStart;
            if (copyStart >= 0 && copyStart < state.output.length) {
                const previewLen = Math.min(16, op.copyLength, state.output.length - copyStart);
                const copyPreview = Array.from(state.output.slice(copyStart, copyStart + previewLen)).map(formatAscii).join('');
                details += ` "${copyPreview}${op.copyLength > 16 ? '...' : ''}"`;
            }
            elements.tipVal.textContent = details;
        }

        elements.tipOp.textContent = `dst=${op.dstOffset.toLocaleString()} len=${op.length}`;

        elements.tooltip.style.left = (e.clientX + 15) + 'px';
        elements.tooltip.style.top = (e.clientY + 15) + 'px';
        elements.tooltip.classList.add('visible');

        elements.canvasInfo.textContent = `[${x}, ${y}] = ${bytePos}`;
    } else {
        elements.tooltip.classList.remove('visible');
        elements.canvasInfo.textContent = '';
    }
});

elements.canvasWrapper.addEventListener('mouseleave', () => {
    elements.tooltip.classList.remove('visible');
});

// Filter buttons
elements.filterBtns.forEach(btn => {
    btn.addEventListener('click', () => {
        const type = btn.dataset.type;
        state.filters[type] = !state.filters[type];
        btn.classList.toggle('active', state.filters[type]);
        renderTable();
    });
});

// Window resize
window.addEventListener('resize', () => {
    if (state.output) {
        updateCanvasTransform();
    }
});

// Keyboard shortcuts
document.addEventListener('keydown', (e) => {
    if (!state.output) return;

    if (e.key === '+' || e.key === '=') {
        zoomTo(state.zoom * 2);
    } else if (e.key === '-') {
        zoomTo(state.zoom / 2);
    } else if (e.key === '0') {
        fitToView();
    } else if (e.key === 'ArrowDown' && state.selectedOp !== null) {
        e.preventDefault();
        selectOperation(Math.min(state.selectedOp + 1, state.operations.length - 1));
    } else if (e.key === 'ArrowUp' && state.selectedOp !== null) {
        e.preventDefault();
        selectOperation(Math.max(state.selectedOp - 1, 0));
    }
});
