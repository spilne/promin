import { render } from "preact";
import { App } from "./app.tsx";

const root = document.getElementById("app");
if (!root) throw new Error("No #app root found");
render(<App />, root);
