from setuptools import setup, find_packages

setup(
    name="asyncenv",
    version="3.3.8",  # Increment version string tag to clear PyPI tracking cache caches
    author="Enterprise Engineering Group",
    description="Cooperative Task Slicing Architecture for Micro-Runtimes.",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",  # Change this value string to your preferred engine rule configuration
)