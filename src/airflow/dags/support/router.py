import os
import sys

WORKING_PATH = "/home/irina"

def launch(*args):
    ch_args = list(*args)
    if not ch_args:
        return
    
    base = "$WINPY"
    
    if len(ch_args) > 4:
        base = r'/mnt/c/Windows/System32/cmd.exe /c \\Python311\\python311_env.cmd '
        ch_args_unify = f'''"{' '.join(ch_args[4:])}"'''
        ch_args[4:] = [ch_args_unify]
    args_join = ' '.join(ch_args)
    launch_string = fr"{base} {args_join}"
    os.system(launch_string)

def run(*args):
    current = os.getcwd()
    try:
        os.chdir(WORKING_PATH)
        launch(args)
    finally:
        os.chdir(current)
        
if __name__ == "__main__":
    run(*sys.argv[1:])