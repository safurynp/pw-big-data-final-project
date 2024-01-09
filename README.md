# Getting started
Clone the repository and set up submodules:
1. `git clone https://github.com/safurynp/pw-big-data-final-project.git`
2. `git submodule init`
3. `git submodule update`


## Environment setup
Set up Python environment using Conda:
1. `conda env create -f environment.yml`
2. `conda activate big-data-env`


# Technology stack
## Programming languages
- Python
- Bash
- LaTeX

## Big Data
- Amazon S3
- AWS Glue
- Amazon EMR
- Apache Spark through PySpark

## Software tools
- AWS CLI version 2
- Git
- MacTeX
- Miniconda

## Development Environment
- Visual Studio Code


# Data
Tennis data is obtained from two repositories:
- For men's professional tennis: [https://github.com/JeffSackmann/tennis_atp.git](https://github.com/JeffSackmann/tennis_atp.git)
- For women's professional tennis: [https://github.com/JeffSackmann/tennis_wta.git](https://github.com/JeffSackmann/tennis_wta.git)

Above two repositories are set up as Git submodules within this repository.

All data from these sources is attributed to Jeff Sackmann and is licensed under the terms of Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License.
