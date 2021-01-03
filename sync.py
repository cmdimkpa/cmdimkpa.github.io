# Synchronize Platform API

import subprocess
import datetime

def run_shell(cmd):
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE, shell=True)
    out, err = p.communicate()
    if err:
        return err.decode()
    else:
        out = out.decode()
        try:
            return eval(out)
        except:
            return out

# destination side
DestinationRoot = "/home/ubuntu/updater2/destination/Platform-API"
DestinationUpdateCommand = 'cd %s;sudo git add .;sudo git commit -m "sync @ %s";sudo git push' % (DestinationRoot, str(datetime.datetime.today()))

# source side
SourceRoot = "/home/ubuntu/updater2/source/parallelscore.io"
SourceUpdateCommand = 'cd %s;sudo git checkout main;sudo git pull;sudo cp -r * %s' % (SourceRoot, DestinationRoot)

run_shell(SourceUpdateCommand)
run_shell(DestinationUpdateCommand)
