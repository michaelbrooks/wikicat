import math
from PIL import Image, ImageDraw

class BitMap(object):

    def __init__(self, total):
        self.total = total
        self.image = self.create_image()

    def create_image(self):
        """
        Generates a manipulable in-memory image resource.
        :return:
        """
        # how many categories?
        aspect_ratio = float(4) / 3
        self.width = int(math.sqrt(aspect_ratio * self.total))
        self.height = int(self.width / aspect_ratio)

        img = Image.new("RGB", (self.width, self.height))
        return img

    def map_category(self, category, image):
        """
        Maps the category to a pixel in the image.

        :param category:
        :param image:
        :return:
        """

        row = math.floor(category / self.width)
        col = category % self.width

        return col, row


    def color_numbers(self, numbers, color):
        draw = ImageDraw.Draw(self.image)
        draw.point([self.map_category(num, self.image) for num in numbers], color)


    def save(self, full_path):
        with open(full_path, 'wb') as outfile:
            self.image.save(outfile)
