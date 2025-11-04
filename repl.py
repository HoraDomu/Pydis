from main import Client, CommandError


def main():
    print("Welcome to Pydis Python REPL! Type 'exit' or 'quit' to quit.")
    c = Client()

    while True:
        try:
            cmd = input("> ").strip()
            if cmd.lower() in ("exit", "quit"):
                print("Goodbye!")
                break
            if not cmd:
                continue

            parts = cmd.split()
            cmd_name, *args = parts

            # Convert args to bytes for the protocol
            args_bytes = [arg.encode("utf-8") for arg in args]

            # Try to find a matching method on the client
            if hasattr(c, cmd_name.lower()):
                method = getattr(c, cmd_name.lower())
                try:
                    result = method(*args_bytes)
                    if isinstance(result, list):
                        for i, val in enumerate(result):
                            print(f"{i}) {val.decode() if val else None}")
                    elif isinstance(result, bytes):
                        print(result.decode())
                    else:
                        print(result)
                except CommandError as e:
                    print(f"(error) {e}")
            else:
                # fallback: send as raw command if not directly implemented
                try:
                    result = c.execute(cmd_name, *args)
                    print(result)
                except CommandError as e:
                    print(f"(error) {e}")

        except KeyboardInterrupt:
            print("\nExiting...")
            break
        except EOFError:
            print("\nExiting...")
            break
        except Exception as e:
            print(f"Unexpected error: {e}")


if __name__ == "__main__":
    main()
