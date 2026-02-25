#!/usr/bin/env python

import os
import argparse
import yaml
import xml.etree.ElementTree as ET
from xml.dom import minidom


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Build a Label Studio Labeling Interface from an annotations config yaml and xml template')

    parser.add_argument('--template', required=True, metavar='XML', help='Label Interface Template File')
    parser.add_argument('--config', required=True, metavar='YAML', help='Annotation fields yaml')

    parser.add_argument('--outdir', default='.', help='Output directory for downloaded task-data csv')
    parser.add_argument('--outfile',
                        help='Output CSV filename. By default a filename based on the project and snapshot timestamp is used')

    return parser.parse_args()


def yaml_to_taxonomy_xml(yaml_string):
    data = yaml.safe_load(yaml_string)

    fields = data["fields"]
    xml_elements = []

    for name, attribs in fields.items():

        include_header = attribs.pop('INCLUDE_HEADER')
        field_type = attribs.pop('TYPE')

        if include_header and ('placeholder' in attribs or 'header' in attribs):
            header = ET.Element("Header")
            if 'header' in attribs:
                header.set("value", attribs['header'])
            elif 'placeholder' in attribs:
                header.set("value", attribs['placeholder'])
            xml_elements.append(header)

        choices = attribs.pop('choices') if 'choices' in attribs else []

        annotation_attrs = dict(name=name, toName=attribs.pop('toName'))
        annotation_attrs.update(attribs)

        annotation = ET.Element(field_type, annotation_attrs)

        for choice_str in choices:
            alias, value = choice_str.split(":", 1)
            choice = ET.Element("Choice", dict(alias=alias.strip(), value=choice_str))
            annotation.append(choice)

        # indents choices when later turned to string
        ET.indent(annotation, space='  ')

        xml_elements.append(annotation)

    return xml_elements


def prettify_xml(xml_elements, indent=4):
    indent_str = ' ' * indent
    xml_strings = [ET.tostring(elem, encoding="utf-8", short_empty_elements=elem.tag == 'Header').decode() for elem in
                   xml_elements]
    xml_block = '\n'.join(xml_strings)
    xml_block = indent_str + xml_block.replace('\n', '\n' + indent_str)  # add indentation to all lines
    return xml_block


def main():
    # Parse command-line arguments
    args = parse_arguments()

    with open(args.config) as f:
        annotation_fields_yaml_config = f.read()

    xml_elements = yaml_to_taxonomy_xml(annotation_fields_yaml_config)
    xml_block = prettify_xml(xml_elements, indent=4)
    xml_block = xml_block.replace('</Taxonomy>', '</Taxonomy>\n')  # additional lines

    with open(args.template) as f:
        xml_template = f.read()

    full_xml = xml_template.format(ANNOTATIONS=xml_block)

    # check okay
    assert minidom.parseString(full_xml)

    # Save XML
    if args.outfile:
        output_path = os.path.join(args.outdir, args.outfile)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w') as f:
            f.write(full_xml)
        print(f'Labeling Interface XML written to: "{output_path}"')

    else:
        print(full_xml)

    # TODO conenct with LS instance to CHECK existing annotation fields
    #      make sure only ADDING new fields or editing placeholder

    # TODO pull and download config as yaml, probably a separate script


if __name__ == '__main__':
    main()

