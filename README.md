# parquet-reader

Goal: write a client-side, browser-based reader of [parquet](https://parquet.apache.org/) files leveraging [web assembly](https://webassembly.org/) and recent developments
in [arrow2](https://github.com/jorgecarleitao/arrow2) and [parquet2](https://github.com/jorgecarleitao/parquet2).

How:

* build a `wasm` package based on `parquet2` that can read a parquet file from bytes.
* create a react app that uses that library and offer a UI to load a file from the computer and navigate on it (header, footer, groups, pages)
* **do not** interact with any server (other than serving the original JS/HTML): only use client-side code so that users can depend on this without concerns of data exfiltration, etc.

Down the road:
* expose method to read the file from s3 using some authentication model
* expose method to read the file from azure using some authentication model
* offer UI to explore the _data_ on the file (e.g. using web assembly and `arrow2`)

Probable tech stack:
* Rust for parquet reading
* web-pack `.wasm -> js-equivalent`
* react to UI and usual frontend stuff
* material UI for visualization
* AWS cloudfront for deployment

## How to build

```
cd parquet-reader
wasm-pack build --out-dir ../wasm-build
cd ..
cd client
npm start
```

open the terminal on the browser and observe an error.
