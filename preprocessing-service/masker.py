# Standard Library
import logging
import re
from typing import List

logger = logging.getLogger(__name__)


class MaskingInstruction:
    def __init__(self, regex_pattern: str, mask_with: str):
        self.regex_pattern = regex_pattern
        self.mask_with = mask_with
        self.regex = re.compile(regex_pattern)
        self.mask_with_wrapped = "<" + mask_with + ">"


class RegexMasker:
    def __init__(
        self,
        masking_instructions: List[MaskingInstruction],
        masking_instructions_before_value_assign_token_split: List[MaskingInstruction],
    ):
        self.masking_instructions = masking_instructions
        self.masking_instructions_before_value_assign_token_split = (
            masking_instructions_before_value_assign_token_split
        )
        self.delimiters = r'([|:| \(|\)|\[|\]\'|\{|\}|"|,|=])'
        self.remove_delimiters = r'([| \(|\)|\[|\]\'|\{|\}|"|,])'
        self.ansi_escape = re.compile(r"(\x9B|\x1B\[)[0-?]*[ -\/]*[@-~]")

    def mask(self, content: str, is_control_plane_log: bool):

        # get rid of escape keys
        content = self.ansi_escape.sub("", content)

        # If log is a control plane log, mask out content from structured control plane log messages which follow the 'a="somecontent" to be a=<a>'
        if is_control_plane_log:
            assignment_regex = '([A-z]+)="(.*?)"'
            matching_indices = [(m.start(0), m.end(0)) for m in re.finditer(assignment_regex, content)]
            modified_content = content[:]

            for m in matching_indices:
                substring = content[m[0]:m[1]]
                left_side_term = substring.split("=\"")[0]
                if left_side_term != "err":
                    modified_content = modified_content.replace(substring, "{}=<{}>".format(left_side_term, left_side_term))
            content = modified_content[:]

        for mi in self.masking_instructions_before_value_assign_token_split:
            # content = re.sub(mi.regex, mi.mask_with_wrapped, content)
            content = mi.regex.sub(mi.mask_with_wrapped, content)

        content = " ".join(re.split(r"([=|:])", content))
        content = " ".join(re.split(r"[\n\r\t\r]", content))

        for mi in self.masking_instructions:
            # content = re.sub(mi.regex, mi.mask_with_wrapped, content)
            content = mi.regex.sub(mi.mask_with_wrapped, content)

        # split on delimiters
        split_content = re.split(self.delimiters, content)
        content = " ".join(
            filter(lambda x: x not in self.remove_delimiters, split_content)
        )

        # convert to lower case
        content = content.lower()

        return content


masking_list = [
    {
        "regex_pattern": "[^\\s]+\.go : [0-9]+",
        "mask_with": "GO_FILE_PATH"
    },
    {
        "regex_pattern": "((?<=[^A-Za-z0-9])|^)(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\/\\d{1,3})((?=[^A-Za-z0-9])|$)",
        "mask_with": "IP",
    },
    {
        "regex_pattern": "((?<=[^A-Za-z0-9])|^)(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})((?=[^A-Za-z0-9])|$)",
        "mask_with": "IP",
    },
    {
        "regex_pattern": "((?<=[^A-Za-z0-9])|^)(\\d+\\.\\d+\\s*(s|ds|cs|ms|µs|ns|ps|fs|as|zs|ys))((?=[^A-Za-z0-9])|$)",
        "mask_with": "DURATION",
    },
    {
        "regex_pattern": "(/[a-zA-Z_\\-\\./\\(?:[0-9]+[a-zA-Z0-9]|[a-zA-Z]+[0-9]\\)]*[\\s]?)",
        "mask_with": "PATH",
    },
    {
        "regex_pattern": "(?:[0-9]+[a-zA-Z!\\(\\)\\-\\.\\?\\[\\]_`~;:!@#$%^&+=\\*]|[a-zA-Z!\\(\\)\\-\\.\\?\\[\\]_`~;:!@#$%^&+=\\*]+[0-9])[a-zA-Z0-9!\\(\\)\\-\\.\\?\\[\\]_`~;:!@#$%^&+=\\*]*",
        "mask_with": "TOKEN_WITH_DIGIT",
    },
    {
        "regex_pattern": "((?<=[^A-Za-z0-9])|^)([\\-\\+]?\\d*\\.?\\d+)((?=[^A-Za-z0-9])|$)",
        "mask_with": "NUM",
    },
    {"regex_pattern": "{[\\s]*}", "mask_with": "EMPTY_SET"},
    {"regex_pattern": "\\[[\\s]*\\]", "mask_with": "EMPTY_LIST"},
]
# email_address_pattern = {"regex_pattern": "[a-z0-9]+[\\._]?[a-z0-9]+[@]\\w+[.]\\w{2,3}","mask_with": "EMAIL_ADDRESS"}

masking_list_before_value_assigning_token_split = [
    {
        "regex_pattern": "(http|ftp|https)://([\\w_-]+(?:(?:\\.*[\\w_-]+)+))([\\w.,@?^=%&:/~+#-]*[\\w@?^=%&/~+#-])?",
        "mask_with": "URL",
    },
    {
        "regex_pattern": "\\d{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[1-2]\\d|3[0-1])[T|\\s](?:[0-1]\\d|2[0-3]):[0-5]\\d:[0-5]\\d(?:\\.\\d+|)[(?:Z|(?:\\+|\\-)(?:\\d{2}):?(?:\\d{2}))]",
        "mask_with": "UTC_DATE",
    },
    {
        "regex_pattern": "[IWEF]\\d{4}\\s\\d{2}:\\d{2}:\\d{2}[\\.\\d+]*",
        "mask_with": "KLOG_DATE",
    },
    {
        "regex_pattern": "(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\\s+(\\d{1,2}) (2[0-3]|[01]?[0-9]):([0-5]?[0-9]):([0-5]?[0-9])",
        "mask_with": "CUSTOM_DATE",
    },
]


class LogMasker:
    def __init__(self):
        masking_instructions = []
        for mi in masking_list:
            instruction = MaskingInstruction(mi["regex_pattern"], mi["mask_with"])
            masking_instructions.append(instruction)

        masking_instructions_before_value_assigning_token_split = []
        for mi in masking_list_before_value_assigning_token_split:
            instruction = MaskingInstruction(mi["regex_pattern"], mi["mask_with"])
            masking_instructions_before_value_assigning_token_split.append(instruction)

        self.masker = RegexMasker(
            masking_instructions,
            masking_instructions_before_value_assigning_token_split,
        )

    def mask(self, content: str, is_control_plane_log: bool):
        return self.masker.mask(content, is_control_plane_log)
