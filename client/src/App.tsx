import React from 'react';

function replacer(key: any, value: any) {
  if (value instanceof Map) {
    return Object.fromEntries(value)
  } else {
    return value;
  }
}

class App extends React.Component<{}, { metadata: any }> {
  constructor(props: any) {
    super(props)
    this.uploadFile = this.uploadFile.bind(this);
    this.state = { metadata: null };
  }

  uploadFile(event: any) {
    let file = event.target.files[0];

    if (file) {
      let self = this;
      import('wasm').then(({ read_parquet }) => {
        var reader = new FileReader();
        reader.onload = function (e) {
          const file_content = new Uint8Array(reader.result as ArrayBuffer);
          const metadata = read_parquet(file_content);
          self.setState({ metadata: metadata });
        }
        reader.readAsArrayBuffer(file);
      })
    }
  }

  render() {
    console.log("state:", this.state.metadata);
    return (
      <div className="App">
        <header className="App-header">
          <p>
            Select a parquet file to read its contents
          </p>
          <input type="file"
            name="myFile"
            onChange={this.uploadFile} />
        </header>
        <pre>{JSON.stringify(this.state.metadata, replacer, 2)}</pre>
      </div>
    );
  }
}

export default App;
