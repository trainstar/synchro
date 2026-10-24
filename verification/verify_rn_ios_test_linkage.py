"""Reject duplicate SDK implementations in the hosted bridge test products."""

from pathlib import Path
import re
import subprocess
import sys


products = Path(__file__).resolve().parents[1] / (
    "clients/react-native/example/ios/build/bridge-transactions/Build/Products/Debug-iphonesimulator"
)
host = products / "SynchroReactNative-TestHost.app"
tests = host / "PlugIns/SynchroReactNative-Unit-Tests.xctest"
expected_classes = {
    f"type metadata for SynchroReactNative.{name}"
    for name in (
        "SynchroModuleImpl", "TransactionSession", "ReactPromiseSettlement",
        "ReactEventTarget", "AuthRequestStore",
    )
}
magic_numbers = {
    bytes.fromhex(value)
    for value in ("feedface", "cefaedfe", "feedfacf", "cffaedfe", "cafebabe", "bebafeca", "cafebabf", "bfbafeca")
}
sdk_class = re.compile(r"^(?:_\$s(?:7Synchro|18SynchroReactNative).*CN|_OBJC_CLASS_\$_SynchroModuleImpl)$")

try:
    if not tests.is_dir():
        raise ValueError("Hosted bridge test bundle is missing")
    definitions = {}
    test_metadata = set()
    inspected = set()
    for entry in sorted(host.rglob("*")):
        if not entry.is_file():
            continue
        binary = entry.resolve()
        if binary in inspected:
            continue
        inspected.add(binary)
        with binary.open("rb") as stream:
            if stream.read(4) not in magic_numbers:
                continue
        output = subprocess.run(
            ["xcrun", "nm", "-U", str(binary)],
            check=True, capture_output=True, text=True,
        ).stdout
        symbols = {line.split()[-1] for line in output.splitlines() if line.split()}
        classes = {symbol for symbol in symbols if sdk_class.fullmatch(symbol)}
        wrapper_classes = {
            symbol for symbol in classes
            if symbol.startswith("_$s18SynchroReactNative") or symbol == "_OBJC_CLASS_$_SynchroModuleImpl"
        }
        if wrapper_classes and not binary.is_relative_to(tests.resolve()):
            raise ValueError(f"Test host links wrapper classes: {entry.relative_to(host)}")
        for symbol in classes:
            if symbol in definitions:
                raise ValueError("Multiple test images define the same SDK class")
            definitions[symbol] = binary
        metadata = sorted(symbol for symbol in classes if symbol.startswith("_$s18SynchroReactNative"))
        if metadata:
            names = subprocess.run(
                ["xcrun", "swift-demangle", "--compact"],
                input="\n".join(metadata), check=True, capture_output=True, text=True,
            ).stdout
            test_metadata.update(names.splitlines())
    if not expected_classes.issubset(test_metadata):
        raise ValueError("Test bundle does not define every required production ownership class")
    if "_OBJC_CLASS_$_SynchroModuleImpl" not in definitions:
        raise ValueError("Test bundle does not define the production Objective-C bridge class")
except (OSError, ValueError, subprocess.CalledProcessError) as error:
    print(f"Bridge test linkage failed: {error}", file=sys.stderr)
    sys.exit(1)

print("Bridge test linkage passed: one definition per SDK class across host and test images")
