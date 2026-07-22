import re
import sys
from typing import List

_LABEL_RE = re.compile(r'^(\s*)__L_.*_End:\s*$')

def post_process(text: str) -> str:
    """
    For each line matching ^\\s*__L_.*_End:$:
      - Add '\\* ' at the very beginning of this line and the next three lines.
      - If the label is '__L_SwitchRecv_End:', after those four lines insert a line '    __Exit();'.
    """
    lines: List[str] = text.splitlines(keepends=True)
    i = 0
    while i < len(lines):
        m = _LABEL_RE.match(lines[i])
        if not m:
            i += 1
            continue

        orig_stripped = lines[i].strip()
        is_switch_recv_end = (orig_stripped == '__L_SwitchRecv_End:')

        start = i
        end = min(i + 4, len(lines))  # process current and next three lines
        for j in range(start, end):
            if not lines[j].startswith('\\*'):
                lines[j] = '\\* ' + lines[j]

        insert_idx = end
        if is_switch_recv_end:
            # Insert after the four processed lines (or fewer if near EOF)
            lines.insert(insert_idx, f'    __Exit();\n')
            i = insert_idx + 1  # skip past inserted line
        else:
            i = end
    return ''.join(lines)

def post_process_file(path: str) -> None:
    with open(path, 'r', encoding='utf-8') as f:
        content = f.read()
    new_content = post_process(content)
    if new_content != content:
        with open(path, 'w', encoding='utf-8') as f:
            f.write(new_content)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print(f'Usage: {sys.argv[0]} <file_path>')
        sys.exit(1)
    post_process_file(sys.argv[1])