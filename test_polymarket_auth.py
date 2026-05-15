import os

from polymarket_us import PolymarketUS


def main():
    key_id = os.environ.get("POLYMARKET_KEY_ID")
    secret_key = os.environ.get("POLYMARKET_SECRET_KEY")

    print("DEBUG env keys found:")
    print("POLYMARKET_KEY_ID found:", bool(key_id))
    print("POLYMARKET_SECRET_KEY found:", bool(secret_key))
    print("POLYMARKET_KEY_ID preview:", key_id[:8] + "..." if key_id else "MISSING")
    print("POLYMARKET_SECRET_KEY length:", len(secret_key) if secret_key else 0)

    if not key_id:
        raise RuntimeError("Missing POLYMARKET_KEY_ID env var")

    if not secret_key:
        raise RuntimeError("Missing POLYMARKET_SECRET_KEY env var")

    client = PolymarketUS(
        key_id=key_id,
        secret_key=secret_key,
    )

    print("Polymarket client initialized successfully")


if __name__ == "__main__":
    main()