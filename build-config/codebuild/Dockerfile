FROM public.ecr.aws/codebuild/amazonlinux2-x86_64-standard:4.0

SHELL ["/bin/bash", "-c"]
RUN yum install gcc zlib-devel bzip2 bzip2-devel readline-devel sqlite sqlite-devel openssl-devel tk-devel libffi-devel tar make which zip unzip patch -y
RUN yum install git -y
# RUN git clone https://github.com/pyenv/pyenv.git /root/.pyenv
RUN echo 'export PYENV_ROOT="/root/.pyenv"' >> /root/.bash_profile
RUN echo 'export PATH="$PYENV_ROOT/bin:$PATH"' >> /root/.bash_profile
RUN echo -e 'if command -v pyenv 1>/dev/null 2>&1; then\n  eval "$(pyenv init --path)"\nfi' >> /root/.bash_profile
ENV PYENV_ROOT=/root/.pyenv
ENV PATH=$PYENV_ROOT/bin:$PATH
RUN source /root/.bash_profile

RUN pyenv install 3.7.10
RUN pyenv install 3.9.13

RUN echo -e '#!/usr/bin/env bash\nset -e\n[ -n "$PYENV_DEBUG" ] && set -x\n\nprogram="${0##*/}"\n\nexport PYENV_ROOT="/root/.pyenv"\nexec "/root/.pyenv/libexec/pyenv" exec "$program" "$@"' > /root/.pyenv/shims/aws
RUN chmod +x /root/.pyenv/shims/aws

RUN pyenv global 3.7.10
RUN /root/.pyenv/versions/3.7.10/bin/python3.7  -m pip install --upgrade pip
RUN /root/.pyenv/versions/3.7.10/bin/python3.7  -m pip install awscli

RUN pyenv global 3.9.13
RUN /root/.pyenv/versions/3.9.13/bin/python3.9  -m pip install --upgrade pip
RUN /root/.pyenv/versions/3.9.13/bin/python3.9  -m pip install awscli
RUN /root/.pyenv/versions/3.9.13/bin/python3.9  -m pip install sceptre
