from datetime import datetime
from os import system

commands = '''
git config user.name "Automated"
git config user.email "actions@users.noreply.github.com"
git add -A
git commit -m "Latest data: {0} ({1})" || exit 0
git push
'''.strip()
for command in commands.format(datetime.utcnow().strftime('%d %B %Y %H:%M'), 0).splitlines():
    system(command)
