import base64
import importlib
import json
import zlib
from datetime import datetime
from typing import Callable, Any, Optional


def as_string(value: Any) -> str:
    """Convert value to string."""
    if isinstance(value, bytes):
        return value.decode('utf-8')
    return str(value)


class SafeJSONEncoder(json.JSONEncoder):
    """安全的 JSON 编码器，支持 tuple 类型"""
    def encode(self, obj):
        return super().encode(self._preprocess(obj))
    
    def _preprocess(self, obj):
        if isinstance(obj, tuple):
            return {'__type__': 'tuple', 'items': [self._preprocess(item) for item in obj]}
        elif isinstance(obj, list):
            return [self._preprocess(item) for item in obj]
        elif isinstance(obj, dict):
            return {key: self._preprocess(value) for key, value in obj.items()}
        return obj


def safe_json_decoder(obj):
    """安全的 JSON 解码器，还原 tuple 类型"""
    if isinstance(obj, dict):
        if obj.get('__type__') == 'tuple':
            return tuple(safe_json_decoder(item) for item in obj['items'])
        return {key: safe_json_decoder(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [safe_json_decoder(item) for item in obj]
    return obj


def safe_loads(value: bytes) -> Any:
    """安全反序列化：使用 JSON + Base64 替代 pickle
    
    防止任意代码执行攻击，只支持基本数据类型
    """
    if value is None or value == b'null':
        return None
    try:
        json_str = base64.b64decode(value).decode('utf-8')
        return json.loads(json_str, object_hook=safe_json_decoder)
    except Exception as e:
        raise ValueError(f'Failed to deserialize value: {e}')


# 压缩相关的常量
COMPRESSION_THRESHOLD = 1024  # 超过1KB才压缩
COMPRESSION_LEVEL = 6  # 压缩级别 1-9，6是平衡速度和压缩率


def compress_data(data: bytes) -> bytes:
    """
    压缩数据，小数据直接返回
    
    Args:
        data: 原始数据
        
    Returns:
        压缩后的数据（带有压缩标记）
    """
    if len(data) < COMPRESSION_THRESHOLD:
        # 小数据不压缩，添加标记
        return b'\x00' + data
    
    # 压缩数据并添加标记
    compressed = zlib.compress(data, level=COMPRESSION_LEVEL)
    return b'\x01' + compressed


def decompress_data(data: bytes) -> bytes:
    """
    解压数据
    
    Args:
        data: 带有压缩标记的数据
        
    Returns:
        解压后的原始数据
    """
    if not data:
        return data
    
    # 检查压缩标记
    if data[0:1] == b'\x00':
        # 未压缩
        return data[1:]
    elif data[0:1] == b'\x01':
        # 已压缩
        return zlib.decompress(data[1:])
    else:
        # 兼容旧数据（无标记）
        return data


def safe_dumps_with_compression(obj: Any) -> bytes:
    """
    安全序列化并压缩
    
    对于大数据自动启用压缩，减少存储和传输开销
    """
    if obj is None:
        return b'null'
    
    try:
        json_str = SafeJSONEncoder(ensure_ascii=False, separators=(',', ':')).encode(obj)
        data = json_str.encode('utf-8')
        return compress_data(data)
    except (TypeError, ValueError) as e:
        raise TypeError(f'Object of type {type(obj).__name__} is not JSON serializable: {e}')


def safe_loads_with_compression(data: bytes) -> Any:
    """
    解压并安全反序列化
    """
    if data is None or data == b'null':
        return None
    
    try:
        # 解压
        decompressed = decompress_data(data)
        json_str = decompressed.decode('utf-8')
        return json.loads(json_str, object_hook=safe_json_decoder)
    except Exception as e:
        raise ValueError(f'Failed to deserialize value: {e}')


def import_attribute(name: str) -> Callable[..., Any]:
    """Returns an attribute from a dotted path name. Example: `path.to.func`.

    When the attribute we look for is a staticmethod, module name in its
    dotted path is not the last-before-end word

    E.g.: package_a.package_b.module_a.ClassA.my_static_method

    Thus we remove the bits from the end of the name until we can import it

    Args:
        name (str): The name (reference) to the path.

    Raises:
        ValueError: If no module is found or invalid attribute name.

    Returns:
        Any: An attribute (normally a Callable)
    """
    name_bits = name.split('.')
    module_name_bits, attribute_bits = name_bits[:-1], [name_bits[-1]]
    module = None
    while len(module_name_bits):
        try:
            module_name = '.'.join(module_name_bits)
            module = importlib.import_module(module_name)
            break
        except ImportError:
            attribute_bits.insert(0, module_name_bits.pop())

    if module is None:
        # maybe it's a builtin
        try:
            return __builtins__[name]  # type: ignore[index]
        except KeyError:
            raise ValueError('Invalid attribute name: %s' % name)

    attribute_name = '.'.join(attribute_bits)
    if hasattr(module, attribute_name):
        return getattr(module, attribute_name)
    # staticmethods
    attribute_name = attribute_bits.pop()
    attribute_owner_name = '.'.join(attribute_bits)
    try:
        attribute_owner = getattr(module, attribute_owner_name)
    except:  # noqa
        raise ValueError('Invalid attribute name: %s' % attribute_name)

    if not hasattr(attribute_owner, attribute_name):
        raise ValueError('Invalid attribute name: %s' % name)
    return getattr(attribute_owner, attribute_name)


def get_now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
