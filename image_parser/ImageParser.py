from exif import Image

# Source: https://medium.com/spatial-data-science/how-to-extract-gps-coordinates-from-images-in-python-e66e542af354
# Translated tutorial into this class

class ImageParser:
    def read_image_get_coordinates(self, filepath):
        lat = -1
        lon = -1
        img = self.__open_image(filepath)
        if not img.has_exif:
            print("img has no exif information")
            return (lat, lon)

        if (not hasattr(img, 'gps_latitude')) or (not hasattr(img, 'gps_longitude')):
            print("Image metadata has no coordinates")
            return (lat, lon)
        else:
            lat = self.__convert_dms_to_dd(img.gps_latitude, img.gps_latitude_ref)
            lon = self.__convert_dms_to_dd(img.gps_longitude, img.gps_longitude_ref)
        return (lat, lon)

    def __open_image(self, filepath):
        with open(filepath, 'rb') as src:
            img = Image(src)
        return img

    def __convert_dms_to_dd(self, dms_coordinates, dms_coordinate_ref):
        dd_coordinate = dms_coordinates[0] + dms_coordinates[1] / 60 + dms_coordinates[2] / 3600
        if dms_coordinate_ref == "S" or dms_coordinate_ref == "W":
            dd_coordinate = -dd_coordinate
        return dd_coordinate