import React from 'react';
import logo from './logo.svg';
import './App.css';

function App() {
  import('wasm').then(({ read_parquet }) => {
    const file_content = new Uint8Array([0, 1, 0]);
    const file_version = read_parquet(file_content);
    console.log("parquet file version", file_version);
  })
  return (
    <div className="App">
      <header className="App-header">
        <img src={logo} className="App-logo" alt="logo" />
        <p>
          Edit <code>src/App.tsx</code> and save to reload.
        </p>
        <a
          className="App-link"
          href="https://reactjs.org"
          target="_blank"
          rel="noopener noreferrer"
        >
          Learn React
        </a>
      </header>
    </div>
  );
}

export default App;
