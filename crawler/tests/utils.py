import os

from scrapy.http import HtmlResponse, XmlResponse, Request

def get_set_dict(dictionary):
    result_dict = {}
    for key, value in dictionary.items():
        if isinstance(value, list):
            result_dict[key] = set(value)
        elif isinstance(value, dict):
            result_dict[key] = get_set_dict(value)
        else:
            result_dict[key] = value

    return result_dict

def fake_xml_response_from_file(file_name, url):
    """
    Create a Scrapy fake HTTP response from a HTML file
    @param file_name: The relative filename from the responses directory,
                      but absolute paths are also accepted.
    @param url: The URL of the response.
    returns: A scrapy HTTP response which can be used for unittesting.
    """

    request = Request(url=url)
    if not file_name[0] == '/':
        responses_dir = os.path.dirname(os.path.realpath(__file__))
        file_path = os.path.join(responses_dir, file_name)
    else:
        file_path = file_name

    with open(file_path, 'r') as f:
        file_content = f.read()

        response = XmlResponse(url=url,
            request=request,
            body=file_content,
            encoding = 'utf-8'
        )

        return response

def fake_html_response_from_file(file_name, url):
    """
    Create a Scrapy fake HTTP response from a HTML file
    @param file_name: The relative filename from the responses directory,
                      but absolute paths are also accepted.
    @param url: The URL of the response.
    returns: A scrapy HTTP response which can be used for unittesting.
    """

    request = Request(url=url)
    if not file_name[0] == '/':
        responses_dir = os.path.dirname(os.path.realpath(__file__))
        file_path = os.path.join(responses_dir, file_name)
    else:
        file_path = file_name

    with open(file_path, 'r') as f:
        file_content = f.read()

        response = HtmlResponse(url=url,
            request=request,
            body=file_content,
            encoding = 'utf-8'
        )

        return response