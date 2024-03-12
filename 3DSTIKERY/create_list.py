import os
from aspose.imaging import Image
from aspose.imaging.imageoptions import *

# You can get all image templates from https://github.com/aspose-imaging/Aspose.Imaging-for-Python-Net/blob/master/Examples/data/Templates.zip
# After download archive please unpack it and replace templatesFolder variable path with your path to unpacked archive folder
# get path of the input data
templates_folder = os.environ["DATA_PATH"] if "DATA_PATH" in os.environ else "data"
print(templates_folder)
# get output path
output_folder = os.environ["OUT_PATH"] if "OUT_PATH" in os.environ else "out"
print(output_folder)
# Load the cdr file in an instance of Image
with Image.load(os.path.join(templates_folder, "шаблон.cdr")) as image:
  # Create an instance of PngOptions
  export_options = PngOptions()
  vector_options = CdrRasterizationOptions()
  vector_options.page_width = image.width
  vector_options.page_height = image.height
  export_options.vector_rasterization_options = vector_options

  # Save cdr to png
  image.save(os.path.join(output_folder, "cdr-to-png-output.png"), export_options)