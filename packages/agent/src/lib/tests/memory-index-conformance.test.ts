import { InMemoryMemoryIndex } from "../memory-index.ts";
import { memoryIndexTestSuite } from "../../testing.ts";

memoryIndexTestSuite(() => new InMemoryMemoryIndex());
