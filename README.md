# nano_merger

Tools to merge root files from custom nano productions.

```mermaid
flowchart TD
    GatherFiles --> ValidateEvents
    GatherFiles --> ComputeMergingFactor
    GatherFiles --> MergeFiles
    ComputeMergingFactor --> MergeFiles
    MergeFiles --> CreateEntry
```
