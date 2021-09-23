FROM alpine:3.12
RUN echo "https://dl-3.alpinelinux.org/alpine/v3.12/main" >> /etc/apk/repositories
RUN echo "https://dl-3.alpinelinux.org/alpine/v3.12/community" >> /etc/apk/repositories
RUN apk add --no-cache su-exec bash wget git python3 py3-pip py3-numpy py3-numpy-f2py py3-scipy py3-geos py3-matplotlib py3-lxml tiff expat curl ca-certificates openssl gcompat libgcc proj proj-util freetype hdf5 netcdf
RUN apk add --no-cache --virtual build-deps python3-dev build-base swig tiff-dev expat-dev eigen-dev boost-dev hdf5-dev netcdf-dev
RUN rm -f /usr/bin/python && ln -s /usr/bin/python3.8 /usr/bin/python
WORKDIR /root

RUN pip3 install -U pip && pip3 install wheel

RUN mkdir -p pyke
WORKDIR /root/pyke
RUN wget "http://downloads.sourceforge.net/project/pyke/pyke/1.1.1/pyke3-1.1.1.zip?r=&ts=1403707359" -O pyke3-1.1.1.zip
RUN unzip pyke3-1.1.1.zip
RUN rm ./pyke3-1.1.1.zip
WORKDIR /root/pyke/pyke-1.1.1
RUN pip3 install .
WORKDIR /root

RUN mkdir -p udunits2
WORKDIR /root/udunits2
RUN wget ftp://ftp.unidata.ucar.edu/pub/udunits/udunits-2.2.28.tar.gz
RUN tar -xf ./udunits-2.2.28.tar.gz
RUN rm ./udunits-2.2.28.tar.gz
WORKDIR udunits-2.2.28
RUN ./configure --prefix=/usr
RUN make && make install
WORKDIR /root
RUN rm -rf /root/udunits2

RUN apk add --no-cache --virtual build-deps2 py3-numpy-dev geos-dev proj-dev
RUN echo "manylinux1_compatible = False" >> /usr/lib/python3.8/_manylinux.py &&\
    echo "manylinux2010_compatible = False" >> /usr/lib/python3.8/_manylinux.py &&\
    echo "manylinux2014_compatible = False" >> /usr/lib/python3.8/_manylinux.py &&\
    pip3 install "Cython" &&\
    pip3 install "netcdf4==1.5.3" "cartopy>=0.12.0" &&\
    python3 --version &&\
    rm -rf /usr/lib/python3.8/_manylinux.py
ADD requirements.txt .
RUN pip3 install -r "requirements.txt"
RUN git clone --depth 1 --branch "v2.4.0" "https://github.com/SciTools/iris.git"
WORKDIR /root/iris
RUN python3 setup.py std_names pyke_rules
RUN python3 setup.py install
WORKDIR /root
RUN rm -rf /root/iris
RUN mkdir -p ./src
ADD transformation src/transformation
ADD bjp src/bjp
WORKDIR /root/src/transformation
RUN bash -c "python3 setup.py build_ext --inplace"
WORKDIR /root/src/bjp
RUN bash -c "python3 setup.py build_ext --inplace --include-dirs=/usr/include/eigen3/"
WORKDIR /
RUN apk del build-deps2 && apk del build-deps
RUN wget "https://github.com/krallin/tini/releases/download/v0.18.0/tini-static-muslc-amd64" -O /tini-static &&\
    chmod +x /tini-static
RUN mkdir -p workflow
RUN chmod -R 777 workflow
WORKDIR /workflow
ADD workflow ./
WORKDIR /
RUN cp /root/src/transformation/pytrans.cpython*.so . &&\
    cp /root/src/bjp/pybjp.cpython*.so .
ADD entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/tini-static", "--", "/entrypoint.sh"]
CMD ["python3", "-m", "workflow"]
