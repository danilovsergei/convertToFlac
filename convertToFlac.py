#!/usr/bin/python
import argparse
import glob
import logging
import logging.config
import shlex
import subprocess
from subprocess import call, check_output

import codecs
import os
import re
import shutil
import tempfile
from os import listdir
from os.path import isfile, join

TMP_DIR_PREFIX = 'cueconvert_'
logger = logging.getLogger(__name__)
log_level = 'INFO' # default log level.

SUPPORTED_SOURCE_FORMATS = ['m4a', 'wav', 'ape', 'wv']

class CueAlbum:
    def __init__(self):
        self.album_tag = None
        self.artist_tag = None
        self.year_tag = None

        self.cue_disks = []
        self.header = []

    def append_cue_disc(self, cue_disc):
        self.cue_disks.append(cue_disc)

    def append_line_to_header(self, line):
        self.header.append(line)
        self._populate_album_tag(line)
        self._populate_artist_tag(line)
        self._populate_year_tag(line)

    def _populate_album_tag(self, line):
        match = re.match('^TITLE (.*)$', line)
        if match:
            # Remove optionally present quotes
            self.album_tag = shlex.split(match.group(1))[0]

    def _populate_artist_tag(self, line):
        match = re.match('^PERFORMER (.*)$', line)
        if match:
            self.artist_tag = shlex.split(match.group(1))[0]

    def _populate_year_tag(self, line):
        match = re.match('^REM DATE (.*)$', line)
        if match:
            self.year_tag = shlex.split(match.group(1))[0]

    def get_disc(self, id):
        return self.cue_disks[id]

    def get_last_disc(self):
        if not self.cue_disks:
            raise RuntimeError("No cue discs added yet")
        return self.cue_disks[-1]

class CueDisc:
    def __init__(self):
        self.titles_tags = []
        self.cue_context = []
        self.music_file_name = None

    def append_to_cue_context(self, line):
        self.cue_context.append(line)
        self._populate_title_tag(line)

    def _populate_title_tag(self, line):
        match = re.match('([ \t]+)?TITLE (.*)$', line)
        if match:
            self.titles_tags.append(shlex.split(match.group(2))[0])

class Converter:
    def __init__(self):
        self.src_dir = os.path.abspath(args.src_dir)
        os.chdir(self.src_dir)
        self.dest_dir = args.dest_dir
        self.scan_recursively = not args.only_top_dir

    def __get_dest_dir(self, current_dir):
        if os.path.isabs(self.dest_dir):
            return self.dest_dir
        else:
            return os.path.join(current_dir, self.dest_dir)

    def convert(self):
        if not args.debug:
            FileUtils.clean_up_old_dirs(self.src_dir)
        files_dict = FileUtils.scan_directory(self.src_dir, self.scan_recursively)
        if FileUtils.hasExtension(files_dict, 'cue') and not args.ignore_cue_files:
            for cue_file in files_dict['cue']:
                src_dir = os.path.dirname(cue_file)
                CueConverter(cue_file, src_dir, self.__get_dest_dir(src_dir)).convert()
        else:
            for ext in SUPPORTED_SOURCE_FORMATS:
                if ext in files_dict:
                    for file in files_dict[ext]:
                        self.__convert_single_file(file, self.__get_dest_dir(os.path.dirname(file)))

    def __convert_single_file(self, src_file, dest_dir):
        if not os.path.exists(dest_dir):
            os.makedirs(dest_dir)
        filename = os.path.basename(os.path.splitext(src_file)[0])
        destFile = os.path.join(dest_dir, filename + '.flac')
        logging.info('Writing flac file: %s', destFile)
        call(["ffmpeg", "-i", src_file, destFile])

class CueConverter():
    FIRST_TRACK_START_NUMBER = 1

    def __init__(self, cue_file, src_dir, dest_dir):
        self.cue_file = cue_file
        self.src_dir = src_dir
        self.dest_dir = dest_dir

        self.cue_album = self.parse_cue_file()

    def convert(self):
        first_track_num = CueConverter.FIRST_TRACK_START_NUMBER
        for cue_disc_id, cue_disc in enumerate(self.cue_album.cue_disks):
            # Each single physical cue file may have more than one discs inside it
            # referenced as FILE "name (LP1).flac" WAVE
            # shnsplit does not support multiple cue discs inside single cue..
            #
            # Create temp cue file for each disc.
            #Besides that it also fixes problem with incorrect new line separators
            # when file was generated on Windows.
            temp_cue = self.create_temp_cue_file(self.cue_album.header + cue_disc.cue_context)
            try:
                music_file_path = join(self.src_dir, cue_disc.music_file_name)
                # create temp dir to make sure it has only shnsplit generated files
                # it's necessary for tagging since we don't have direct filename mapping
                # between shnsplit output and cue file
                temp_dir = tempfile.mkdtemp(dir=self.src_dir, prefix=TMP_DIR_PREFIX)
                self.split_file_by_cue_sheet(temp_cue, music_file_path, temp_dir, first_track_num)
                self.remove_pregap_files(temp_dir)
                CueToFlacTagUtils.tag_files(temp_dir, self.cue_album, cue_disc_id)
                first_track_num += len(cue_disc.titles_tags)
                FileUtils.move_to_newdir(temp_dir, self.dest_dir)
            finally:
                os.remove(temp_cue)
                if not args.debug:
                  shutil.rmtree(temp_dir)

    def remove_pregap_files(self, temp_dir):
        """shnsplit sometimes generates pregap files  which creates
        inconsistency between track numbers in cue and actual files in the dir.
        it confuses tagging algorithm. Just remove it since it useless.
        """
        for pregap_file in glob.glob(os.path.join(glob.escape(temp_dir), '*pregap.flac')):
            os.remove(pregap_file)

    def parse_cue_file(self):
        logging.info('Read cue file: %s' + self.cue_file)
        cue_album = CueAlbum()
        for line in FileUtils.readTextFile(self.cue_file):
            if line.startswith("FILE "):
                cue_album.append_cue_disc(CueDisc())
                cue_album.get_last_disc().music_file_name = shlex.split(line)[1]
            if len(cue_album.cue_disks) > 0:
                line = CueConverter.fix_time_format(line)
                cue_album.get_last_disc().append_to_cue_context(line)
            else:
                # still reading header
                cue_album.append_line_to_header(line)
        return cue_album

    def create_temp_cue_file(self, cue_content_array):
        temp_cue = tempfile.NamedTemporaryFile(suffix='.cue', delete=False)
        for line in cue_content_array:
            temp_cue.write(line.encode('utf-8'))
            temp_cue.write(os.linesep.encode('utf-8'))
        temp_cue.close()
        if not os.path.getsize(temp_cue.name):
            raise IOError('Temp cue file {}  is emty'.format(temp_cue))
        return temp_cue.name

    def split_file_by_cue_sheet(self, cue_file_path, music_file_path, out_dir, first_track_num):
        logging.info('Split cue file for %s:' + music_file_path)
        cmd = [
            'shnsplit', '-f', cue_file_path,
            '-t', '%n. %t' ,
            '-c',  str(first_track_num),
            '-d', out_dir,
            '-o', 'flac',
            '-O',  "always" ,
            music_file_path]
        logging.info('Split cmd: %s',  ' '.join(cmd))
        try:
            out  = check_output(cmd,universal_newlines=True,stderr=subprocess.STDOUT)
            logging.info(out)
        except subprocess.CalledProcessError as e:
            logging.error('Failed to split cue %s for %s : %s', cue_file_path, music_file_path, e.output)

    # workaround shnsplit: error: m:ss.ff format can only be used with CD-quality files
    @staticmethod
    def fix_time_format(line):
        if line.strip().startswith('INDEX '):
            # TODO replace with regex matcher based on groups
            line_as_list = line.replace(os.linesep, '').split(':')
            line_as_list[-1] = line_as_list[-1] + '0' + os.linesep
            return ':'.join(line_as_list)
        else:
            return line

class FileUtils():
    @staticmethod
    def move_to_newdir(src_dir, dest_dir):
        if os.path.exists(dest_dir) and os.path.isfile(dest_dir):
            raise AttributeError(
                'Could not create destination directory {}.'
                'There is file exists with similar name'.format(dest_dir))
        if not os.path.exists(dest_dir):
            os.makedirs(dest_dir)
        for file in listdir(src_dir):
            shutil.move(os.path.join(src_dir, file), os.path.join(dest_dir, file))

    @staticmethod
    def clean_up_old_dirs(src_dir):
        old_dirs = glob.glob(os.path.join(glob.escape(src_dir), TMP_DIR_PREFIX+'*'))
        for old_dir in old_dirs:
            shutil.rmtree(old_dir)

    @staticmethod
    def hasExtension(files_dict, ext):
        return ext in files_dict

    @staticmethod
    def readTextFile(text_file, encoding='utf-8'):
        f = codecs.open(text_file, "r", encoding=encoding)
        try:
            return f.read().splitlines()
        except UnicodeDecodeError:
            if encoding != 'utf-8':
                raise IOError(
                    'Failed to read file {} with fallback encoding {}. \n'
                    'This cue is unsupported or has wrong format'.format(text_file, encoding))
            if not args.fallback_cue_encoding:
                raise IOError(
                    'Failed to read file {} as unicode. \n'
                    'Please specify fallback encoding using --fallback_cue_encoding'.format(text_file))
            return FileUtils.readTextFile(text_file, args.fallback_cue_encoding)
        finally:
            f.close()

    @staticmethod
    def scan_directory(src_dir, recursive):
        files_dict = {}
        FileUtils.__scan_directory_rec(src_dir, files_dict, recursive)
        return files_dict

    @staticmethod
    def __scan_directory_rec(src_dir, files_dict, recursive):
        for f in listdir(src_dir):
            full_file_name = join(src_dir, f)
            if isfile(full_file_name):
                file_ext = os.path.splitext(full_file_name)[1].lower()[1:]
                if file_ext in files_dict:
                    files_dict[file_ext].append(full_file_name)
                else:
                    files_dict[file_ext] = [full_file_name]
            else:
                if recursive:
                    FileUtils.__scan_directory_rec(full_file_name, files_dict, recursive)


class CueToFlacTagUtils():
    @staticmethod
    def tag_files(files_dir, cue_album, current_disc_id):
        files = glob.glob(os.path.join(glob.escape(files_dir), '*.*'))
        files.sort(key=os.path.getmtime)
        for track_id, file in enumerate(files):
            CueToFlacTagUtils.tag_single_file(file, cue_album, current_disc_id, track_id)

    @staticmethod
    def __add_if_present(cmd, param, tag):
        if tag:
            cmd.append('='.join([param, tag]))

    @staticmethod
    def tag_single_file(flac_file, cue_album, disc_id, track_id):
        cmd = ['metaflac', '--preserve-modtime']
        CueToFlacTagUtils.__add_if_present(cmd,  '--set-tag=ARTIST', cue_album.artist_tag)
        CueToFlacTagUtils.__add_if_present(cmd, '--set-tag=ALBUM', cue_album.album_tag)
        CueToFlacTagUtils.__add_if_present(cmd, '--set-tag=TITLE', cue_album.get_disc(disc_id).titles_tags[track_id])
        CueToFlacTagUtils.__add_if_present(cmd, '--set-tag=DATE', cue_album.year_tag)
        cmd.append(flac_file)
        logging.info('Tag flac file command: %s', ' '.join(cmd))
        call(cmd)

def parse_ags():
    parser = argparse.ArgumentParser(
        usage='use "%(prog)s --help" for more information',
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=os.linesep.join([
            'Examples:',
            '-src_dir=/music/cue --dest_dir=/music/flac',
            '  takes everything recursively from /music/cue and converts it to /music/flac.'
            '  all underlying directory structure will be flatten when absolute dest_dir path specified',
            '--src_dir=/music/cue --dest_dir=flac',
            '  takes everything from /music/cue and converts it to /music/cue/flac',
            '  each subdirectory will have it own flac dir when relative dest_dir specified',
            '--src_dir=/music/cue --dest_dir=flac --fallback_cue_encoding=cp1251',
            ' forces converter to read cue file using cp1251 encoding if it fails to read it as unicode.',
            '--src_dir=/music/cue --dest_dir=flac --only_top_dir',
            ' only the files in /music/cue will be scanned.'
            ' all subdirs will be egnored'
        ])
    )
    parser.add_argument("--src_dir",
                        required=True,
                        help='\n'.join([
                            'Source directory with audio files.',
                            'Any relative path provided will be resolved relatively to script execution directory'
        ])
                        )
    parser.add_argument("--dest_dir",
                        required=True,
                        help='\n'.join([
                        'output directory with flac files.',
                        'Any relative path provided will be resolved relatively to the source directory.'
                        ])
                        )

    parser.add_argument("--fallback_cue_encoding",
                        help='\n'.join([
                            'By default all files assumed to be utf-8 encoded.',
                            'Provided encoding will be used only file read as utf-8 failed'
                        ])
                        )

    parser.add_argument("--ignore_cue_files",
                        action="store_true",
                        default=False,
                        help=
                        '\n'.join([
                        'If True Any found cue files will be ignored.',
                        'While all other music files including ones',
                        '  complementing to cue  will be converted.'
                        ])
                        )

    parser.add_argument("--debug",
                        action="store_true",
                        default=False,
                        help=
                        '\n'.join([
                            'Keeps all temp dirs if True.',
                        ])
                        )

    parser.add_argument("--only_top_dir",
                        action="store_true",
                        default=False,
                        help=
                        '\n'.join([
                            'Scans directories recursively. Default value is True.',
                        ])
                        )

    return parser.parse_args()

def get_logger_config(level):
    return {
        'disable_existing_loggers': False,
        'version': 1,
        'formatters': {
            'short': {
                'format': '%(asctime)s %(levelname)s %(name)s: %(message)s'
            },
        },
        'handlers': {
            'console': {
                'level': level,
                'formatter': 'short',
                'class': 'logging.StreamHandler',
            },
        },
        'loggers': {
            '': {
                'handlers': ['console'],
                'level': level,
            },
            'plugins': {
                'handlers': ['console'],
                'level': level,
                'propagate': False
            }
        },
    }

if __name__ == "__main__":
    args = parse_ags()
    if args.debug:
        log_level = 'DEBUG'
    logging.config.dictConfig(get_logger_config(log_level))
    Converter().convert()

# shntool doc with supported formats
# http://www.etree.org/shnutils/shntool/support/doc/shntool.pdf
