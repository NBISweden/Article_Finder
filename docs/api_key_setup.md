

## Web of Science API key
### How to get a Web of Science API key

You can request access from Clarivate here:

- <https://developer.clarivate.com/apis/wos>

This is the recommended API for this project because it provides access to full record metadata from Web of Science.

### Recommendation

When requesting access, ask for access to **Web of Science API Expanded**.

If your institution has the appropriate Web of Science subscription, this is the API intended for full item-level metadata retrieval.

### After you receive the API key
The fetch workflows require a Web of Science API key.

It supports two ways to provide the key:

1. **Keyring** (**recommended**)
2. **Environment variable / `.env` file**

### Recommended option: keyring

Keyring is the preferred method.With keyring, the secret is stored in the operating system’s credential store instead of being written directly into the project directory.

### Fallback option: `.env`

You can also store the key in a local `.env` file using:

```text
WOS_API_KEY=your_key_here
```

However, this is **less secure** because the key is stored as plain text.

This can increase the risk of accidental exposure, especially if:

- the file is committed by mistake
- the project directory is shared
- local tools or AI coding assistants inspect project files

---

## Save the API key using keyring
### Step 1 — Save the key

Run:

```bash
pixi run python -c "import keyring; from getpass import getpass; keyring.set_password('wos_api','default', getpass('Enter WoS API key: ')); print('saved')"
```

You will be prompted to enter the Web of Science API key.

Paste the key and press Enter.

### Step 2 — Verify that it was saved correctly

Run:

```bash
pixi run python -c "import keyring; v=keyring.get_password('wos_api','default'); print('exists:', bool(v), 'length:', len(v) if v else 0)"
```

### Step 3 — Start the interface

Run:

```bash
pixi run ui
```

If the key is available in keyring, the application will use it automatically.
