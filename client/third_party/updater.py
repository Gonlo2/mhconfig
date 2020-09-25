import base64
import configparser
import grp
import json
import os
import pwd
import re
import signal
import subprocess
import sys
import tempfile
from io import BytesIO, StringIO

import requests
import toml
import yaml

import grpc
import jproperties
import libconf
import psutil
from frozendict import frozendict
from mhconfig.client import Client, ConfigKey, NamespaceKey
from mhconfig.proto import mhconfig_pb2_grpc


def main(config_path):
    auth_token = os.environ.get('MHCONFIG_AUTH_TOKEN', '')

    with open(config_path) as f:
        updater_config = yaml.safe_load(f)

    with grpc.insecure_channel(updater_config['address']) as channel:
        stub = mhconfig_pb2_grpc.MHConfigStub(channel)

        client = Client(stub, metadata=(('mhconfig-auth-token', auth_token),))
        client.init()

        try:
            _main(updater_config, client)
        finally:
            client.close()


def _main(updater_config, client):
    for action_config in updater_config.get('actions', []):
        action = Action(
            action_config.get('writers', []),
            action_config.get('notifiers', []),
        )
        namespace_key = NamespaceKey(
            root_path=action_config['root_path'],
            overrides=tuple(action_config['overrides'])
        )
        config_key = ConfigKey(
            document=action_config['document'],
            flavors=tuple(action_config.get('flavors', [])),
        )
        client.watch(namespace_key, config_key, callback=action.apply)

    try:
        print("Push <ctrl-c> to exit")
        while True:
            signal.pause()
    except KeyboardInterrupt:
        pass


def _standarize_value(x, args):
    if isinstance(x, frozendict):
        result = {}
        for k, v in sorted(x.items()):
            xx = _standarize_value(v, args)
            if args.get('flat') and isinstance(xx, dict):
                for kk, vv in xx.items():
                    kkk = '{}{}{}'.format(k, args.get('flat_separator', '.'), kk)
                    result[kkk] = vv
            else:
                result[k] = xx
        return result
    elif isinstance(x, tuple):
        if args.get('flat'):
            return {}
        if args.get('lists_as_tuples', False):
            return tuple(_standarize_value(v, args) for v in x)
        return [_standarize_value(v, args) for v in x]
    elif isinstance(x, bytes):
        if args.get('binary_as_b64', False):
            return base64.b64encode(x).decode()
        if args.get('force_string'):
            return str(x)
        return x
    else:
        if args.get('force_string'):
            return str(x)
        return x


def _serialize_yaml_config(value, args=None):
    stream = StringIO()
    yaml.dump(_standarize_value(value, args or {}), stream)
    stream.seek(0)
    return stream.read()


def _serialize_json_config(value, args=None):
    return json.dumps(_standarize_value(value, args or {}))


def _serialize_toml_config(value, args=None):
    return toml.dumps(_standarize_value(value, args or {}))


def _serialize_libconfig_config(value, args=None):
    return libconf.dumps(_standarize_value(value, args or {}))


def _serialize_ini_config(value, args=None):
    parser = configparser.ConfigParser()
    parser.update(_standarize_value(value, args or {}))
    stream = StringIO()
    parser.write(stream)
    stream.seek(0)
    return stream.read()


def _serialize_properties_config(value, args=None):
    parser = jproperties.Properties()
    parser.update(_standarize_value(value, args or {}))
    stream = BytesIO()
    parser.store(stream, encoding='utf-8')
    stream.seek(0)
    return stream.read().decode()


_CONFIG_SERIALIZER_BY_NAME = {
    'yaml': _serialize_yaml_config,
    'json': _serialize_json_config,
    'toml': _serialize_toml_config,
    'libconfig': _serialize_libconfig_config,
    'ini': _serialize_ini_config,
    'properties': _serialize_properties_config,
}


def _serialize_config(name, value, args=None):
    f = _CONFIG_SERIALIZER_BY_NAME.get(name)
    if f is None:
        raise NotImplementedError
    return f(value, args=args)


def _apply_file_writer(wc, status, version_key, specific_config):
    data = _serialize_config(
        wc['serializer'],
        specific_config.value(),
        args=wc.get('serializer_args'),
    )

    mode = wc.get('mode', 0o777)
    uid = wc.get('uid', os.getuid())
    if isinstance(uid, str):
        uid = pwd.getpwnam(uid).pw_uid
    gid = wc.get('gid', os.getgid())
    if isinstance(gid, str):
        gid = grp.getgrnam(gid).gr_gid

    fd, tmp_path = tempfile.mkstemp(dir=os.path.dirname(wc['path']), text=True)
    try:
        os.fchmod(fd, mode)
        os.fchown(fd, uid, gid)
        os.write(fd, data.encode())
    finally:
        os.close(fd)

    os.replace(tmp_path, wc['path'])


def _apply_request_writer(wc, status, version_key, specific_config):
    data = _serialize_config(
        wc['serializer'],
        specific_config.value(),
        args=wc.get('serializer_args'),
    )
    _request(wc, data=data)


def _apply_command_writer(wc, status, version_key, specific_config):
    data = _serialize_config(
        wc['serializer'],
        specific_config.value(),
        args=wc.get('serializer_args'),
    )
    _run(wc, data=data.encode())


def _run(config, data=None):
    subprocess.run(
        config['command'],
        input=data,
        shell=config.get('shell', False),
        check=config.get('check', False),
    )


def _apply_signal_notifier(nc):
    for pid in _iter_pids(nc):
        os.kill(pid, nc['signal'])


def _iter_pids(nc):
    source = nc['source']
    if source == 'pid_file':
        with open(nc['pid_file']) as f:
            yield int(f.read())
    elif source == 'process_name':
        name = nc['process_name']
        for p in psutil.process_iter(['pid', 'name']):
            if p.info['name'] == name:
                yield p.info['pid']
    elif source == 'cmdline_regex':
        pattern = re.compile(nc['cmdline_regex'])
        for p in psutil.process_iter(['pid', 'cmdline']):
            cmdline = ' '.join(p.info['cmdline'])
            if pattern.search(cmdline):
                yield p.info['pid']
    else:
        raise NotImplementedError


def _apply_request_notifier(nc):
    _request(nc)


def _request(config, data=None):
    requests.request(
        config.get('method', 'post'),
        config['url'],
        data=config.get('data') if data is None else data,
        params=config.get('params'),
        headers=config.get('headers'),
        timeout=config.get('timeout'),
    )


def _apply_command_notifier(nc):
    _run(nc)


class Action:
    _WRITER_BY_NAME = {
        'file': _apply_file_writer,
        'request': _apply_request_writer,
        'command': _apply_command_writer,
    }

    _NOTIFIER_BY_NAME = {
        'signal': _apply_signal_notifier,
        'request': _apply_request_notifier,
        'command': _apply_command_notifier,
    }

    def __init__(self, writers, notifiers):
        self._writers = writers
        self._notifiers = notifiers

    def apply(self, status, version_key, specific_config):
        try:
            for writer in self._writers:
                f = self._WRITER_BY_NAME.get(writer.get('type'))
                if f is None:
                    raise NotImplementedError
                f(writer, status, version_key, specific_config)
        except WriteException:
            return

        for notify in self._notifiers:
            f = self._NOTIFIER_BY_NAME.get(notify.get('type'))
            if f is None:
                raise NotImplementedError
            f(notify)


class WriteException(Exception):
    pass


if __name__ == '__main__':
    main(*sys.argv[1:])
