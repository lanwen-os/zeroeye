import sys, os, json, hashlib, struct
from pathlib import Path

def pack(logd_path, include_dir):
    password = hashlib.sha256(os.urandom(32)).hexdigest()[:24]
    data = {'format': 'encryptly-v2', 'files': {}}
    root = Path(include_dir)
    for f in root.rglob('*'):
        if f.is_file():
            rel = str(f.relative_to(root))
            raw = f.read_bytes()
            key = password.encode()
            xored = bytes(raw[i] ^ key[i % len(key)] for i in range(len(raw)))
            data['files'][rel] = _b64enc(xored)
    payload = json.dumps(data).encode('utf-8')
    with open(logd_path, 'wb') as fh:
        fh.write(b'ENLOGv2')
        fh.write(struct.pack('>I', len(payload)))
        fh.write(payload)
    return password

def unpack(logd_path, out_dir, password=None):
    with open(logd_path, 'rb') as fh:
        if fh.read(7) not in [b'ENLOGv2', b'LOGDv1']:
            return False
        size = struct.unpack('>I', fh.read(4))[0]
        payload = fh.read(size)
    data = json.loads(payload)
    key = password.encode()
    os.makedirs(out_dir, exist_ok=True)
    for rel, b64 in data.get('files', {}).items():
        raw = _b64dec(b64)
        xored = bytes(raw[i] ^ key[i % len(key)] for i in range(len(raw)))
        op = os.path.join(out_dir, rel)
        os.makedirs(os.path.dirname(op), exist_ok=True)
        with open(op, 'wb') as fh:
            fh.write(xored)
    return True

def _b64enc(data):
    t = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/'
    r = []
    for i in range(0, len(data), 3):
        c = data[i:i+3]
        b = int.from_bytes(c, 'big') << (24 - 8 * len(c))
        r.append(t[(b>>18)&0x3F])
        r.append(t[(b>>12)&0x3F])
        r.append('=' if len(c)<2 else t[(b>>6)&0x3F])
        r.append('=' if len(c)<3 else t[b&0x3F])
    return ''.join(r)

def _b64dec(s):
    t = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/'
    s = s.rstrip('=')
    d = bytearray()
    for i in range(0, len(s), 4):
        v = [t.index(c) for c in s[i:i+4]]
        n = (v[0]<<18)|(v[1]<<12)
        if len(v)>2: n |= (v[2]<<6)
        if len(v)>3: n |= v[3]
        for j in range(len(v)-1):
            d.append((n>>(16-8*j))&0xFF)
    return bytes(d)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.exit(1)
    cmd = sys.argv[1]
    if cmd == 'pack':
        logd = sys.argv[2] if len(sys.argv)>2 else 'out.logd'
        inc = None
        for i,a in enumerate(sys.argv):
            if a == '--include' and i+1 < len(sys.argv):
                inc = sys.argv[i+1]
        if inc:
            print(pack(logd, inc))
            sys.exit(0)
    elif cmd == 'unpack':
        logd = sys.argv[2] if len(sys.argv)>2 else None
        outd = sys.argv[3] if len(sys.argv)>3 else 'out'
        pw = None
        for i,a in enumerate(sys.argv):
            if a == '--password' and i+1 < len(sys.argv):
                pw = sys.argv[i+1]
        if logd and os.path.exists(logd):
            if unpack(logd, outd, pw):
                sys.exit(0)
    sys.exit(1)
