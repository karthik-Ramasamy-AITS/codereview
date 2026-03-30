from re import match
from airflow.exceptions import AirflowException
from airflow.models.variable import Variable
import re, datetime, string, random, os, logging
from airflow.operators.python import get_current_context

log_config = Variable.get("amgl_logging_config", deserialize_json=True)
is_loginfo_enabled = log_config['log_info']
is_logerror_enabled = log_config['log_error']
is_logdebug_enabled = log_config['log_debug']
needs_highlighting = log_config['needs_highlighting']
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

class StringMask:
    MASK_STATIC_CHARS = 0
    MASK_REGEX_SELECT = 1
    MASK_REGEX_REPLACE = 2
    MASK_CUSTOM_NUMBER = 3
    MASK_CUSTOM_TIMESTAMP = 4
    TERMINUS_REGEX_SELECT = '$'
    TERMINUS_CUSTOM = '#'
    TERMINUS_NONE = ' '

    def __init__(self, mask):
        self.mask = mask
        self.mask_parts = []
        self.parse_mask()

    def parse_mask(self):
        part_type = self.MASK_STATIC_CHARS
        old_part_type = self.MASK_STATIC_CHARS
        finished = False
        current_terminus = self.TERMINUS_NONE
        current_mask_part = ""
        i = 0

        while i < len(self.mask):
            if part_type == self.MASK_STATIC_CHARS:
                if self.mask[i] == self.TERMINUS_REGEX_SELECT:
                    old_part_type = self.MASK_STATIC_CHARS
                    current_terminus = self.TERMINUS_REGEX_SELECT
                    part_type = self.MASK_REGEX_SELECT
                    finished = True
                elif self.mask[i] == self.TERMINUS_CUSTOM:
                    old_part_type = self.MASK_STATIC_CHARS
                    current_terminus = self.TERMINUS_CUSTOM
                    if self.mask[i + 1] == 'N':
                        part_type = self.MASK_CUSTOM_NUMBER
                    elif self.mask[i + 1] == 'T':
                        part_type = self.MASK_CUSTOM_TIMESTAMP
                    finished = True
                else:
                    current_mask_part += self.mask[i]

            elif self.mask[i] == current_terminus:
                finished = True
                old_part_type = part_type
                part_type = self.MASK_STATIC_CHARS
                current_terminus = self.TERMINUS_NONE
            else:
                current_mask_part += self.mask[i]

            if i + 1 == len(self.mask) and not finished:
                old_part_type = part_type
                finished = True

            if finished:
                if old_part_type == self.MASK_STATIC_CHARS:
                    if current_mask_part:
                        self.mask_parts.append(StaticMaskPart(current_mask_part))
                elif old_part_type == self.MASK_REGEX_SELECT:
                    self.mask_parts.append(RegexSelectMaskPart(current_mask_part))
                elif old_part_type == self.MASK_CUSTOM_NUMBER:
                    self.mask_parts.append(EnumerateMaskPart(current_mask_part))
                elif old_part_type == self.MASK_CUSTOM_TIMESTAMP:
                    self.mask_parts.append(TimestampMaskPart(current_mask_part))
                current_mask_part = ""
                finished = False

            i += 1

    def apply(self, source):
        s = []
        for mask_part in self.mask_parts:
            mask_part.set_output(s)
            mask_part.apply(source)
        return ''.join(s)


class MaskPart:
    def __init__(self):
        self.s = []

    def set_output(self, s):
        self.s = s

    def apply(self, source):
        pass


class StaticMaskPart(MaskPart):
    def __init__(self, static_chars):
        super().__init__()
        self.static_chars = static_chars

    def apply(self, source):
        self.s.append(self.static_chars)


class RegexSelectMaskPart(MaskPart):
    def __init__(self, regex):
        super().__init__()
        if not regex:
            regex = ".*"
        self.regex = regex
        self.pattern = re.compile(regex)

    def apply(self, source):
        match = self.pattern.search(source)
        if match:
            self.s.append(match.group())


class CustomMaskPart(MaskPart):
    def __init__(self, mask):
        super().__init__()
        self.mask = mask

    def get_mask(self):
        return self.mask

    def get_custom_mask_type_flag(self):
        return self.mask[0]


class TimestampMaskPart(CustomMaskPart):
    def __init__(self, mask):
        super().__init__(mask)
        self.format = mask.split(",")[1] if "," in mask else "%Y-%m-%d %H:%M:%S"

    def apply(self, source):
        current_datetime = datetime.datetime.now().strftime(self.format)
        self.s.append(current_datetime)


class EnumerateMaskPart(CustomMaskPart):
    def __init__(self, mask):
        super().__init__(mask)
        self.index = 1

    def apply(self, source):
        self.index += 1
        self.s.append(str(self.index - 1))

class RegexStringMask:
    def __init__(self, regex, replacement, instance_id):
        self.regex = regex
        self.replacement = replacement
        self.instance_id = instance_id

    def apply(self, source):
        replacer = re.sub(self.regex, self.replacement, source)
        p = re.compile(r'(\{TIME.+?\})')
        m = p.finditer(replacer)
        for match in m:
            date_pattern = re.sub(r'(\{TIME-|\})', '', match.group())
            replacer = replacer.replace(date_pattern, self.get_current_datetime(date_pattern))
        replacer = re.sub(r'(\{TIME-|\})', '', replacer)
        pattern = re.compile(r'\{INSTANCEID')
        replacer = re.sub(pattern, self.instance_id, replacer)
        return replacer

    @staticmethod
    def get_current_datetime(format_string):
        current_datetime = datetime.datetime.now()
        formatted_datetime = current_datetime.strftime(format_string)
        return formatted_datetime

def filter_files(list_of_files = [], file_filter=None):
    error = None
    try:
        logDebug(f'Plugin :Started matching the Pattern {file_filter} for list of files {list_of_files}')
        if file_filter is None:
            matching_files = list_of_files
            logDebug('Plugin : file_filter sent as None returning original')
        elif list_of_files is None:
            matching_files = []
            logDebug('Plugin : list_of_files sent as None returning []')
        else:
            if file_filter == "*.*":
                logInfo(f'file_filter is converted to ".*"')
                file_filter = ".*"
            matching_files = list(filter(lambda v: match(file_filter, v), list_of_files))
            logInfo(f'Plugin : matching_files are {matching_files}')
    except Exception as e:
        logError(f'Plugin : exception while filtering {e}')
        error = e
        raise AirflowException(f"Error while exception while filtering , error: {e}")
    return matching_files, error

def rename_files(list_of_files = [], regex=None, replacement=None, instance_id=None, rename_mask=None):
    rename_map = {}
    error = None
    try:
        if rename_mask is not None and len(rename_mask) == 0:
             rename_mask = None
        if replacement is None and rename_mask is not None:
          logDebug(f'Plugin : Processing for rename mask since replcement is None, rename_mask is {rename_mask}')
          s = StringMask(rename_mask)
          for file in list_of_files:
              rename_map [file] = s.apply(file)
              logDebug(f'Plugin : file rename applied {rename_map[file]}')
          return rename_map, error
        logDebug(f'Plugin :Started matching the Pattern {regex} for list of files {list_of_files}')
        if regex is None:
            rename_map = create_map(list_of_files)
            logDebug('Plugin : Regex sent as None returning original')
        elif replacement is None:
            rename_map = create_map(list_of_files)
            logDebug('Plugin : replacement sent as None returning original')            
        elif list_of_files is None:
            rename_map = {}
            logDebug('Plugin : list_of_files sent as None returning []')
        else:
            logDebug(f'Plugin : started rename pattern matching')
            if instance_id is None:
                instance_id = generateInstanceId()
            for file in list_of_files:
                logDebug(f'Plugin : actual file {file} - replacement - {replacement} - regex - {regex}')
                replace = RegexStringMask(regex, replacement, instance_id)
                rename_map [file] = replace.apply(file)
        logInfo(f'Plugin : matching_files are {rename_map}')         
    except Exception as e:
        logError(f'Plugin : error is {e}')
        error = e
        raise AirflowException(f"Error while creating rename map , error: {e}")
    return rename_map, error

def create_map(list_of_files):
	map = {}
	for file in list_of_files:
		map [file] = file
	logDebug(f'Plugin : map is {map}')
	return map

def logInfo(message):
	if(needs_highlighting):
		logger.info('*********************************************************************************')
	if(is_loginfo_enabled):
		logger.info('Info : Template version v2 '+message)

def logDebug(message):
	if(needs_highlighting):
		logger.info('*********************************************************************************')
	if(is_logdebug_enabled):
		logger.info('Debug : Template version v2 '+message)

def logError(message):
	if(needs_highlighting):
		logger.error('*********************************************************************************')
	if(is_logerror_enabled):
		logger.error('Error : Template version v2 '+message)

def generateRandom():
	N = 7
	res = ''.join(random.choices(string.ascii_uppercase +
                             string.digits, k=N))
	logDebug("Plugin: The generated random string : " + str(res))
	return res

def handleFailures(continue_on_failure):
    status = False
    logDebug(f'Plugin: handleFailures initiated')
    context = get_current_context()
    email_on_failure = context["dag"].default_args["email_on_failure"]
    retries = context["dag"].default_args["retries"]
    current_retry_no = int(os.environ["AIRFLOW_CTX_TRY_NUMBER"])
    logDebug(f'Plugin : Retry no {current_retry_no}')
    logDebug(f'Plugin : retries {retries}')
    if email_on_failure is True and current_retry_no > retries:
        logDebug(f'Plugin : Max retries reached hence need to send email')
    if continue_on_failure is True and current_retry_no > retries:
        status = True
    #logDebug(f'Plugin : Environment Configs email {os.environ["AIRFLOW_CTX_DAG_EMAIL"]}, dag id {os.environ["AIRFLOW_CTX_DAG_ID"]}, task id {os.environ["AIRFLOW_CTX_TASK_ID"]} , time {os.environ["AIRFLOW_CTX_EXECUTION_DATE"]} , retry no {os.environ["AIRFLOW_CTX_TRY_NUMBER"]} , run id {os.environ["AIRFLOW_CTX_DAG_RUN_ID"]}, email_on_failure {email_on_failure} and retries {retries}')
    return status

def generateInstanceId():
    current_time = datetime.datetime.now()
    return current_time.strftime('%S%f')

def update_extension(file, extension):
    file_name, file_extension = os.path.splitext(file)
    if file_name.endswith(extension):
        return file_name
    else:
        return f"{file_name}{extension}"
