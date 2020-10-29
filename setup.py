from distutils.core import setup

setup(
    name = 'scrapingtool',
    version = '1.0',
    package_dir = {'scrapingtool': 'scrapingtool', 'scrapingtool.taskqueue': 'scrapingtool/taskqueue'},
    packages = ['scrapingtool', 'scrapingtool.taskqueue'],
)