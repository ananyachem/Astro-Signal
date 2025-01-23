# Astro-Signal

### Dataset
This dataset contained radiofrequency signals captured by an array of instruments scanning a specific solid angle of the sky. As is common with real-world data, it included a small amount of noise and inherent errors:
- Angular coordinates had an error of ±0.1 degrees.
- Signal frequency had an error of ±0.1 MHz.
- Timestamps had an error of <0.01 seconds (1 standard deviation).

### Objective
The goal of this project was to identify the source with the highest number of signals ("blips") recorded in the data log. This target source was characterized by:
- Appearing in the same location (within error bounds) in the sky.
- Operating at the same frequency (within error bounds).
- Emitting signals regularly over time during its active period.

A typical pattern might look like:
...blip...blip...blip...blip...blip...blip...
