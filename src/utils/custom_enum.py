class CustomEnum:
    @classmethod
    def to_dict(cls):
        class_attributes = {}
        for attribute_name in dir(cls):
            if not attribute_name.startswith("__") and not attribute_name.startswith("_"):
                attribute_value = getattr(cls, attribute_name)
                if not callable(attribute_value):
                    class_attributes[attribute_name] = attribute_value
        return class_attributes
