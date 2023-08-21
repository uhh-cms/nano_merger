# nano_merger

Tools to merge root files from custom nano productions.

```mermaid
flowchart TD
    UnpackFiles --> GatherFiles
    GatherFiles --> ValidateEvents
    GatherFiles --> ComputeMergingFactor
    GatherFiles --> MergeFiles
    ComputeMergingFactor --> MergeFiles
    MergeFiles --> CreateEntry
```
