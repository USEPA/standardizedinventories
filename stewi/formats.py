"""Define and generate format specs for StEWI inventories."""

from enum import Enum, unique
from pathlib import Path
from stewi.globals import paths
import stewi.exceptions


@unique
class StewiFormat(Enum):
    """Define available formats for StEWI inventories."""

    FLOW = 1
    FACILITY = 2
    FLOWBYFACILITY = 3
    FLOWBYPROCESS = 4

    def __str__(self):
        return self.name.lower()

    def from_str(label):
        """Return class object based on string."""
        if label.lower() in format_dict.keys():
            return StewiFormat[label.upper()]
        else:
            raise stewi.exceptions.StewiFormatError

    def specs(self):
        """Return dictionary of format specifications."""
        return format_dict[str(self)]

    def fields(self):
        """Return list of fields."""
        return [f for f in self.specs().keys()]

    def field_types(self):
        """Return dictionary of fields and dtypes."""
        return {key: value[0]['dtype'] for key, value
                in self.specs().items()}

    def required_fields(self):
        """Return dictionary of fields and dtypes for required fields."""
        return {key: value[0]['dtype'] for key, value
                in self.specs().items() if value[1]['required'] is True}

    def subset_fields(self, df):
        """Return list of fields in format found in df."""
        return [f for f in self.specs().keys() if f in df]

    def path(self):
        """Return local path for directory."""
        return Path(paths.local_path) / str(self)


def ensure_format(f):
    if isinstance(f, StewiFormat):
        return f
    else:
        return StewiFormat.from_str(f)


flowbyfacility_fields = {'FacilityID': [{'dtype': 'str'}, {'required': True}],
                         'FlowName': [{'dtype': 'str'}, {'required': True}],
                         'Compartment': [{'dtype': 'str'}, {'required': True}],
                         'FlowAmount': [{'dtype': 'float'}, {'required': True}],
                         'Unit': [{'dtype': 'str'}, {'required': True}],
                         'DataReliability': [{'dtype': 'float'}, {'required': True}],
                         }

facility_fields = {'FacilityID': [{'dtype': 'str'}, {'required': True}],
                   'FacilityName': [{'dtype': 'str'}, {'required': False}],
                   'Address': [{'dtype': 'str'}, {'required': False}],
                   'City': [{'dtype': 'str'}, {'required': False}],
                   'State': [{'dtype': 'str'}, {'required': True}],
                   'Zip': [{'dtype': 'str'}, {'required': False}],
                   'Latitude': [{'dtype': 'float'}, {'required': False}],
                   'Longitude': [{'dtype': 'float'}, {'required': False}],
                   'County': [{'dtype': 'str'}, {'required': False}],
                   'NAICS': [{'dtype': 'str'}, {'required': False}],
                   'SIC': [{'dtype': 'str'}, {'required': False}],
                   'UrbanRural': [{'dtype': 'str'}, {'required': False}],
                   }

flowbyprocess_fields = {'FacilityID': [{'dtype': 'str'}, {'required': True}],
                        'FlowName': [{'dtype': 'str'}, {'required': True}],
                        'Compartment': [{'dtype': 'str'}, {'required': True}],
                        'FlowAmount': [{'dtype': 'float'}, {'required': True}],
                        'Unit': [{'dtype': 'str'}, {'required': True}],
                        'DataReliability': [{'dtype': 'float'}, {'required': True}],
                        'Process': [{'dtype': 'str'}, {'required': True}],
                        'ProcessType': [{'dtype': 'str'}, {'required': False}],
                        }

flow_fields = {'FlowName': [{'dtype': 'str'}, {'required': True}],
               'FlowID': [{'dtype': 'str'}, {'required': True}],
               'CAS': [{'dtype': 'str'}, {'required': False}],
               'Compartment': [{'dtype': 'str'}, {'required': False}],
               'Unit': [{'dtype': 'str'}, {'required': False}],
               }

format_dict = {'flowbyfacility': flowbyfacility_fields,
               'flowbyprocess': flowbyprocess_fields,
               'facility': facility_fields,
               'flow': flow_fields}
