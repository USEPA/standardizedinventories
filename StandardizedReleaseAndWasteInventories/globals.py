import os

def set_output_dir(directory):
    outputdir = directory
    if not os.path.exists(outputdir): os.makedirs(outputdir)
    return outputdir

global outputdir
outputdir = set_output_dir('StandardizedReleaseandWasteInventories/output/')


