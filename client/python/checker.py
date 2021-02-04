import argparse
import base64
import json
import os
import sys
import time

import colorama
import grpc
from colorama import Fore, Style

from mhconfig.client import Client, LogLevel, Source, UndefinedElement
from mhconfig.proto import mhconfig_pb2_grpc

_LOG_LEVEL_STR_TO_VALUE = {
    'error': LogLevel.ERROR,
    'warn': LogLevel.WARN,
    'info': LogLevel.INFO,
    'debug': LogLevel.DEBUG,
    'trace': LogLevel.TRACE,
}


def cmd_label(x):
    key_value = x.split('/', 1)
    if len(key_value) != 2:
        raise argparse.ArgumentTypeError("The labels must follow the format <key>/<value>")
    return tuple(key_value)


def cmd_log_level(x):
    level = _LOG_LEVEL_STR_TO_VALUE.get(x)
    if level is None:
        raise argparse.ArgumentTypeError("Unknown log level")
    return level


def create_arg_parser():
    parser = argparse.ArgumentParser(description='Check the config')
    parser.add_argument('--address', default='127.0.0.1:2222',
                        help='address of the server')
    parser.add_argument('root_path',
                        help='root path of the config')
    parser.add_argument('--log-level', default='error', type=cmd_log_level,
                        choices=list(_LOG_LEVEL_STR_TO_VALUE.values()),
                        help='min log level to ask')
    parser.add_argument('document', help='document to ask')
    parser.add_argument('--version', default=0, type=int,
                        help='version to ask, latest by default')
    parser.add_argument('labels', nargs='*', type=cmd_label,
                        help='the labels to ask with the format <key>/<value>')
    return parser


def main():
    parser = create_arg_parser()
    args = parser.parse_args()

    auth_token = os.environ.get('MHCONFIG_AUTH_TOKEN', '')

    channel = grpc.insecure_channel(args.address)
    stub = mhconfig_pb2_grpc.MHConfigStub(channel)

    client = Client(
        stub,
        metadata=(('mhconfig-auth-token', auth_token),)
    )

    element, logs, sources = client.get(
        args.root_path,
        dict(args.labels),
        args.document,
        log_level=args.log_level,
        with_position=True,
    )

    if args.log_level.value >= LogLevel.INFO.value:
        cli_blamer = CliBlamer(sources)
        cli_blamer.print(element)
        print()

    cli_logger = CliLogger(args.root_path, sources)
    for log in logs:
        cli_logger.print(log)

class CliLogger:
    def __init__(self, root_path, sources):
        self._root_path = root_path
        self._sources = sources
        self._files = {}

    def print(self, log):
        self._print_header(log)
        self._print_files(log)

    def _print_header(self, log):
        level_color = self._get_level_color(log.level)
        level_str = log.level.name.lower()
        message = log.message.splitlines()[0]
        line = [Style.BRIGHT]
        if log.position is not None:
            line.append(self._format_file_path(log.position))
            line.append(": ")
        line.append(f"{level_color}{level_str}: {Style.RESET_ALL}")
        line.append(f"{Style.BRIGHT}{message}{Style.RESET_ALL}")
        print(''.join(line))

    def _print_files(self, log):
        position_file = self._get_file_line(log.position)
        if position_file is None:
            print()
            return
        print(position_file)

        origin_file = self._get_file_line(log.origin)
        if origin_file is None:
            print(f"{' ' * log.position.col}{Style.BRIGHT}{Fore.LIGHTGREEN_EX}^{Style.RESET_ALL}")
            return

        arrow = self._format_arrow(log.position, log.origin)
        origin_path = self._format_file_path(log.origin)

        print(arrow)
        print(f"{origin_file.rstrip()}  {Style.DIM}# {origin_path}{Style.RESET_ALL}")
        print()

    def _get_level_color(self, level):
        if level == LogLevel.ERROR:
            return Fore.LIGHTRED_EX
        elif level == LogLevel.WARN:
            return Fore.LIGHTMAGENTA_EX
        elif level == LogLevel.INFO:
            return Fore.LIGHTGREEN_EX
        elif level == LogLevel.DEBUG:
            return Fore.LIGHTCYAN_EX
        elif level == LogLevel.TRACE:
            return f"{Style.DIM}{Fore.WHITE}"
        raise NotImplementedError

    # TODO Avoid open a binary file
    def _get_file_line(self, position):
        if position is None:
            return None
        source = self._sources.get(position.source_id)
        if source is None:
            return None
        path = os.path.join(self._root_path, source.path)
        lines = self._get_file_lines(path)
        if lines is None:
            return f"The file '{path}' don't exists"
        return lines[position.line]

    def _get_file_lines(self, path):
        if path not in self._files:
            try:
                with open(path) as f:
                    lines = f.read().splitlines()
            except FileNotFoundError:
                lines = None
            self._files[path] = lines
        return self._files[path]

    def _format_arrow(self, position, origin):
        line_start = min(position.col, origin.col)
        line_end = max(position.col, origin.col)
        line = []
        for i in range(line_end+1):
            if i < line_start:
                line.append(' ')
            elif i == position.col:
                line.append('^')
            elif i == origin.col:
                line.append('|')
            else:
                line.append('-')
        return f"{Style.BRIGHT}{Fore.LIGHTGREEN_EX}{''.join(line)}{Style.RESET_ALL}"

    def _format_file_path(self, position):
        source = self._sources.get(position.source_id)
        path = 'unknown_file' if source is None else source.path
        checksum = 0xffffffff if source is None else source.checksum
        return f"{checksum:08x}: {path}:{position.line+1}:{position.col+1}"


class CliBlamer:
    def __init__(self, sources):
        self._sources = sources
        self._indent = 4

    def print(self, element):
        max_path_len = self._get_max_path_len(element)
        self._print(element, max_path_len, 0, True, 1)

    def _get_max_path_len(self, e):
        src = self._sources.get(e.pos.source_id) if e.pos else None
        l = 0 if src is None else len(self._make_path(e, src))
        if isinstance(e.val, dict):
            for v in e.val.values():
                l = max(l, self._get_max_path_len(v))
        elif isinstance(e.val, list):
            for v in e.val:
                l = max(l, self._get_max_path_len(v))
        return l

    def _print(self, e, max_path_len, level, is_new_line, line):
        if isinstance(e.val, dict):
            for k, v in sorted(e.val.items()):
                if not is_new_line:
                    sys.stdout.write("\n")
                    line += 1
                is_new_line = True
                x = self._make_info_str(v, max_path_len, line)
                k = json.dumps(k)
                sys.stdout.write(f'{x} {" " * (level * self._indent)}{k}: ')
                line = self._print(v, max_path_len, level+1, False, line)
            if not is_new_line:
                sys.stdout.write("{}\n")
                line += 1
        elif isinstance(e.val, list):
            for v in e.val:
                if not is_new_line:
                    sys.stdout.write("\n")
                    line += 1
                is_new_line = True
                x = self._make_info_str(v, max_path_len, line)
                sys.stdout.write(f'{x} {" " * (level * self._indent)}- ')
                line = self._print(v, max_path_len, level+1, False, line)
            if not is_new_line:
                sys.stdout.write("[]\n")
                line += 1
        else:
            if is_new_line:
                x = self._make_info_str(e, max_path_len, line)
                sys.stdout.write(f'{x} {" " * (level * self._indent)}')
            val = e.val
            if isinstance(val, bytes):
                sys.stdout.write("!!binary ")
                val = base64.encodebytes(e.val).decode().strip()
            if isinstance(val, str):
                lines = val.splitlines()
                if len(lines) > 1:
                    sys.stdout.write("|-\n")
                    for x in lines:
                        line += 1
                        info = self._make_info_str(e, max_path_len, line)
                        sys.stdout.write(
                            f'{info} {" " * (level * self._indent)}{x}\n'
                        )
                else:
                    sys.stdout.write(f'{json.dumps(val)}\n')
            elif isinstance(val, UndefinedElement):
                sys.stdout.write("!undefined ~\n")
            else:
                sys.stdout.write(f'{json.dumps(val)}\n')
            line += 1
        return line

    def _make_info_str(self, e, l, line):
        src = self._sources.get(e.pos.source_id) if e.pos else None
        if src is None:
            src = Source(
                id=e.pos.source_id if e.pos else 0xffffffff,
                path="unknown_file",
                checksum=0x00000000,
            )
        path = self._make_path(e, src)
        return f"{src.checksum:08x} {path.ljust(l)} {line:>4})"

    def _make_path(self, e, src):
        return f"{src.path}:{e.pos.line+1}:{e.pos.col+1}" if e.pos else f"{src.path}:-1:-1"


if __name__ == '__main__':
    colorama.init()
    main()
