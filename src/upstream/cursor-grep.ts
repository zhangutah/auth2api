/** Native Cursor GrepArgs/GrepResult ↔ a declared Codex exec_command.
 * The proxy never searches files: this script runs only inside the Codex client.
 * Cursor 3.27.14 wire reference: agent.v1.Grep* in the installed workbench bundle.
 */
import { agentBytes as b, agentNumber as n } from "./cursor-agent";

const join = (...parts: Buffer[]) => Buffer.concat(parts);
const str = (f: Map<number, Buffer | number>, id: number): string => {
  const v = f.get(id);
  return Buffer.isBuffer(v) ? v.toString("utf8") : "";
};
const quote = (s: string) => "'" + s.replace(/'/g, "'\"'\"'") + "'";
const nonnegative = (v: unknown): v is number =>
  Number.isSafeInteger(v) && Number(v) >= 0;

// Bounded search with explicit, machine-parseable observations. No server-side I/O.
const script = String.raw`
import fnmatch, json, os, pathlib, re, signal, sys
signal.signal(signal.SIGALRM, lambda *_: (_ for _ in ()).throw(TimeoutError('grep exceeded 15 seconds')))
signal.alarm(15)
a=json.loads(sys.argv[1]); mode=a['mode']; root=pathlib.Path(a['path'] or '.').expanduser()
if mode not in ('content','files_with_matches','count'): raise ValueError('unsupported grep output_mode: '+mode)
if a['sort'] not in ('','path','modified'): raise ValueError('unsupported grep sort: '+a['sort'])
if len(a['pattern'])>2048 or len(str(root))>4096: raise ValueError('grep input exceeds limit')
if a['head_limit']<0 or a['offset']<0: raise ValueError('negative grep pagination')
flags=(re.I if a['case_insensitive'] else 0)|re.M|(re.S if a['multiline'] else 0)
rx=re.compile(a['pattern'],flags)
exts={'ts':('.ts','.tsx'),'js':('.js','.jsx','.mjs','.cjs'),'py':('.py',),'json':('.json',),'md':('.md','.mdx'),'go':('.go',),'rs':('.rs',),'c':('.c','.h'),'cpp':('.cc','.cpp','.cxx','.hpp'),'yaml':('.yaml','.yml'),'toml':('.toml',),'sh':('.sh',),'txt':('.txt',)}
if a['type'] and a['type'] not in exts: raise ValueError('unsupported grep type: '+a['type'])
if not root.exists(): raise FileNotFoundError(str(root))
files=[]; truncated=False
if root.is_file(): files=[root]
else:
 for base,dirs,names in os.walk(root):
  dirs[:]=sorted(d for d in dirs if d not in ('.git','node_modules','dist','.next'))
  for name in sorted(names):
   p=pathlib.Path(base)/name
   if not p.is_file(): continue
   if a['glob'] and not (fnmatch.fnmatch(name,a['glob']) or fnmatch.fnmatch(str(p.relative_to(root)),a['glob'])): continue
   if a['type'] and not name.endswith(exts[a['type']]): continue
   files.append(p)
   if len(files)>=4000: truncated=True; break
  if truncated: break
if a['sort']=='modified': files.sort(key=lambda p:p.stat().st_mtime,reverse=not a['sort_ascending'])
else: files.sort(key=str,reverse=not a['sort_ascending'] if a['sort'] else False)
records=[]; counts=[]; total_matches=0
before=min(max(a['context_before'],a['context']),12); after=min(max(a['context_after'],a['context']),12)
for p in files:
 if p.stat().st_size>2000000: truncated=True; continue
 raw=p.read_bytes()
 if b'\x00' in raw: continue
 lines=raw.decode('utf8',errors='replace').splitlines()
 hit=set()
 if a['multiline']:
  data='\n'.join(lines)
  for m in rx.finditer(data):
   start=data.count('\n',0,m.start())
   end=data.count('\n',0,max(m.start(),m.end()-1))
   hit.update(range(start,min(end+1,len(lines))))
 else:
  hit.update(i for i,line in enumerate(lines) if rx.search(line))
 if not hit: continue
 file=str(p.resolve()); counts.append({'file':file,'count':len(hit)}); total_matches+=len(hit)
 for idx in sorted(hit):
  if mode=='content':
   for j in range(max(0,idx-before),min(len(lines),idx+after+1)):
    line=lines[j]; records.append({'file':file,'line_number':j+1,'content':line[:512],'content_truncated':len(line)>512,'is_context_line':j not in hit,'anchor':idx})
 if total_matches>=10000: truncated=True; break
# Content context lines can repeat across hits; retain first occurrence per file/line.
if mode=='content':
 seen=set(); unique=[]
 for rec in records:
  key=(rec['file'],rec['line_number'])
  if key not in seen: seen.add(key); rec.pop('anchor'); unique.append(rec)
 records=unique
elif mode=='files_with_matches': records=[{'file':c['file']} for c in counts]
else: records=counts
limit=min(a['head_limit'] or 50,80); offset=a['offset']; page=records[offset:offset+limit]
truncated=truncated or offset+limit<len(records) or a['head_limit']>80
print(json.dumps({'pattern':a['pattern'],'path':a['path'],'mode':mode,'workspace':str(root.resolve()),'records':page,'total_files':len(counts),'total_matches':total_matches,'total_lines':len(page) if mode=='content' else 0,'truncated':truncated,'head_limit':limit,'offset':offset},ensure_ascii=False))
`;

export function cursorGrepCommand(args: Map<number, Buffer | number>): string {
  const integer = (id: number) => {
    const v = args.get(id);
    return typeof v === "number" && Number.isSafeInteger(v) ? v : 0;
  };
  const input = {
    pattern: str(args, 1),
    path: str(args, 2),
    glob: str(args, 3),
    mode: str(args, 4) || "content",
    context_before: integer(5),
    context_after: integer(6),
    context: integer(7),
    case_insensitive: integer(8) !== 0,
    type: str(args, 9),
    head_limit: integer(10),
    multiline: integer(11) !== 0,
    sort: str(args, 12),
    sort_ascending: integer(13) !== 0,
    offset: integer(16),
  };
  return "python3 -c " + quote(script) + " " + quote(JSON.stringify(input));
}

function error(message: string): Buffer {
  return b(2, b(1, message.slice(0, 10000)));
}

/** A completed, parsed client result is required for success. Never invent matches. */
export function cursorGrepResult(
  args: Map<number, Buffer | number>,
  parsed: { code: number; stdout: string } | undefined,
): Buffer {
  if (!parsed)
    return error("Codex grep did not report a completed exit status");
  if (parsed.code !== 0)
    return error(`Codex grep exited ${parsed.code}: ${parsed.stdout}`);
  try {
    const m = JSON.parse(parsed.stdout);
    if (
      !m ||
      !["content", "files_with_matches", "count"].includes(m.mode) ||
      m.pattern !== str(args, 1) ||
      !Array.isArray(m.records) ||
      ![
        m.total_files,
        m.total_matches,
        m.total_lines,
        m.head_limit,
        m.offset,
      ].every(nonnegative) ||
      typeof m.workspace !== "string" ||
      typeof m.path !== "string"
    )
      return error("Invalid Codex grep result metadata");
    let result: Buffer;
    if (m.mode === "count") {
      if (
        !m.records.every(
          (x: any) => typeof x?.file === "string" && nonnegative(x?.count),
        )
      )
        return error("Invalid Codex grep counts");
      result = b(
        1,
        join(
          ...m.records.map((x: any) => b(1, join(b(1, x.file), n(2, x.count)))),
          n(2, m.total_files),
          n(3, m.total_matches),
          n(4, m.truncated ? 1 : 0),
          n(6, m.head_limit),
          n(7, m.offset),
        ),
      );
    } else if (m.mode === "files_with_matches") {
      if (!m.records.every((x: any) => typeof x?.file === "string"))
        return error("Invalid Codex grep files");
      result = b(
        2,
        join(
          ...m.records.map((x: any) => b(1, x.file)),
          n(2, m.total_files),
          n(3, m.truncated ? 1 : 0),
          n(5, m.head_limit),
          n(6, m.offset),
        ),
      );
    } else {
      const byFile = new Map<string, Buffer[]>();
      for (const x of m.records) {
        if (
          typeof x?.file !== "string" ||
          !nonnegative(x?.line_number) ||
          x.line_number < 1 ||
          typeof x?.content !== "string"
        )
          return error("Invalid Codex grep match");
        const match = b(
          2,
          join(
            n(1, x.line_number),
            b(2, x.content),
            n(3, x.content_truncated ? 1 : 0),
            n(4, x.is_context_line ? 1 : 0),
          ),
        );
        const group = byFile.get(x.file) || [];
        group.push(match);
        byFile.set(x.file, group);
      }
      result = b(
        3,
        join(
          ...[...byFile].map(([file, matches]) =>
            b(1, join(b(1, file), ...matches)),
          ),
          n(2, m.total_lines),
          n(3, m.total_matches),
          n(4, m.truncated ? 1 : 0),
          n(5, 0),
          n(6, m.head_limit),
          n(7, m.offset),
        ),
      );
    }
    return b(
      1,
      join(
        b(1, str(args, 1)),
        b(2, str(args, 2)),
        b(3, m.mode),
        b(4, join(b(1, m.workspace), b(2, result))),
      ),
    );
  } catch {
    return error("Invalid completed Codex grep JSON output");
  }
}
