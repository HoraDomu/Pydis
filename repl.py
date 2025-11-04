from main import Client, CommandError

def main():
    print("Welcome to Pydis Python REPL! Type 'exit' to quit")
    c = Client()

    while True:
        try:
            cmd = input("> ").strip()
            if cmd.lower() in ('exit', 'quit'):
                break
            if not cmd:
                continue

            parts = cmd.split()
            cmd_name, *args = parts

            try:
                args_bytes = [arg.encode('utf-8') for arg in args]
                method = getattr(c, cmd_name.lower())
