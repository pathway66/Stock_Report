import glob, re

# Replace emojis with plain text
emoji_map = {
    '\U0001f52e': '[*]',   # [*]
    '\U0001f4c1': '[F]',   # [F]
    '\U0001f4be': '[DB]',  # [DB]
    '\U0001f4a1': '[!]',   # [!]
    '\u2705': '[OK]',      # [OK]
    '\u274c': '[X]',       # [X]
    '\U0001f389': '[!]',   # [!]
    '\U0001f680': '[>]',   # [>]
    '\U0001f4ca': '[G]',   # [G]
    '\U0001f4dd': '[N]',   # [N]
    '\u23f0': '[T]',       # [T]
    '\U0001f50d': '[S]',   # [S]
    '\u2728': '[+]',       # [+]
    '\U0001f4c8': '[^]',   # [^]
    '\U0001f4c9': '[v]',   # [v]
    '\U0001f4b0': '[$]',   # [$]
    '\u26a0': '[W]',       # [W]
    '\U0001f6a8': '[!]',   # [!]
    '\u2714': '[OK]',      # [OK]
    '\U0001f4e6': '[P]',   # [P]
    '\U0001f4cc': '[>]',   # [>]
    '\U0001f527': '[T]',   # [T]
    '\U0001f4c6': '[D]',   # [D]
    '\U0001f4b5': '[$]',   # [$]
    '\u2b50': '[*]',       # [*]
    '\U0001f534': '[R]',   # [R]
    '\U0001f525': '[H]',   # [H]
    '\u231a': '[T]',       # [T]
    '\U0001f3af': '[>]',   # [>]
    '\U0001f4cb': '[L]',   # [L]
    '\U0001f310': '[W]',   # [W]
    '\U0001f4e1': '[>]',   # [>]
}

files = glob.glob('*.py')
fixed = 0

for fn in files:
    with open(fn, 'r', encoding='utf-8') as f:
        content = f.read()
    
    new_content = content
    for emoji, replacement in emoji_map.items():
        new_content = new_content.replace(emoji, replacement)
    
    # Also remove any remaining non-ASCII emoji characters (safety net)
    # Keep Korean characters (AC00-D7A3, 1100-11FF, 3130-318F, A960-A97F)
    
    if new_content != content:
        with open(fn, 'w', encoding='utf-8') as f:
            f.write(new_content)
        print(f"Fixed: {fn}")
        fixed += 1
    else:
        print(f"Skip (no emoji): {fn}")

print(f"\nDone! {fixed} files fixed.")
