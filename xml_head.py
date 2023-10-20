import xml.etree.ElementTree as ET

# print list of subheaders
def xml_head():
        tree = ET.parse('CMD_FLDS_XML_212731828.xml')
        root = tree.getroot()

    # Initialize a set to store unique headers
        unique_headers = set()

    # Iterate through the XML data
        for app in root.findall('.//App'):
            for element in app:
                unique_headers.add(element.tag)
                for subelement in element:
                    unique_headers.add(subelement.tag)

    # Convert set to a sorted list for consistent ordering
        unique_headers = sorted(list(unique_headers))

    # Print or use the list of unique headers
        print('FLDS', unique_headers)
