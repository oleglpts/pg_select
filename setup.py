from distutils.core import setup, Extension
setup(
    name='pg_select',
    version='1.0',
    ext_modules=[
        Extension(
            'pg_select',
            libraries = ['pq', 'python3.8'],
            library_dirs = ['/usr/local/lib', '/usr/lib/x86_64-linux-gnu'],
            sources=['library.cpp']
        )
    ])
