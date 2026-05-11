"""Build Label Studio labeling-interface XML from a YAML field config."""

import argparse
import os
import xml.etree.ElementTree as ET
from xml.dom import minidom

import yaml


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Build a Label Studio labeling interface from YAML and an XML template")
    parser.add_argument("--template", required=True, metavar="XML",
                        help="Label interface template file")
    parser.add_argument("--config", required=True, metavar="YAML",
                        help="Annotation fields YAML")
    parser.add_argument("--outdir", default=".",
                        help="Output directory")
    parser.add_argument("--outfile",
                        help="Output XML filename. Prints XML when omitted.")
    return parser.parse_args()


def yaml_to_taxonomy_xml(yaml_string):
    data = yaml.safe_load(yaml_string)
    if not isinstance(data, dict) or "fields" not in data:
        raise ValueError("YAML config must contain a top-level `fields` mapping")

    xml_elements = []
    for name, raw_attribs in data["fields"].items():
        attribs = dict(raw_attribs)
        include_header = attribs.pop("INCLUDE_HEADER")
        field_type = attribs.pop("TYPE")

        if include_header and ("placeholder" in attribs or "header" in attribs):
            header = ET.Element("Header")
            if "header" in attribs:
                header.set("value", attribs["header"])
            elif "placeholder" in attribs:
                header.set("value", attribs["placeholder"])
            xml_elements.append(header)

        choices = attribs.pop("choices") if "choices" in attribs else []
        annotation_attrs = dict(name=name, toName=attribs.pop("toName"))
        annotation_attrs.update(attribs)
        annotation = ET.Element(field_type, annotation_attrs)

        for choice_str in choices:
            alias, _ = choice_str.split(":", 1)
            choice = ET.Element("Choice", dict(alias=alias.strip(), value=choice_str))
            annotation.append(choice)

        ET.indent(annotation, space="  ")
        xml_elements.append(annotation)

    return xml_elements


def prettify_xml(xml_elements, indent=4):
    indent_str = " " * indent
    xml_strings = [
        ET.tostring(
            elem,
            encoding="utf-8",
            short_empty_elements=elem.tag == "Header",
        ).decode()
        for elem in xml_elements
    ]
    xml_block = "\n".join(xml_strings)
    return indent_str + xml_block.replace("\n", "\n" + indent_str)


def build_label_config(template_xml: str, yaml_string: str) -> str:
    xml_elements = yaml_to_taxonomy_xml(yaml_string)
    xml_block = prettify_xml(xml_elements, indent=4)
    xml_block = xml_block.replace("</Taxonomy>", "</Taxonomy>\n")
    full_xml = template_xml.format(ANNOTATIONS=xml_block)
    try:
        minidom.parseString(full_xml)
    except Exception as exc:
        raise ValueError("generated labeling config is not valid XML") from exc
    return full_xml


def main():
    args = parse_arguments()

    with open(args.config) as f:
        annotation_fields_yaml_config = f.read()
    with open(args.template) as f:
        xml_template = f.read()

    full_xml = build_label_config(xml_template, annotation_fields_yaml_config)
    if args.outfile:
        output_path = os.path.join(args.outdir, args.outfile)
        output_dir = os.path.dirname(output_path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
        with open(output_path, "w") as f:
            f.write(full_xml)
        print(f'Labeling Interface XML written to: "{output_path}"')
    else:
        print(full_xml)


if __name__ == "__main__":
    main()
