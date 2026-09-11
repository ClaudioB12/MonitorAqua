FROM python:3.10-slim-trixie

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-21-jre-headless \
        # ── LaTeX para nbconvert --to pdf ──
        texlive-xetex \
        texlive-fonts-recommended \
        texlive-latex-extra \
        texlive-plain-generic \
        texlive-science \
        texlive-lang-spanish \
        # ── Pandoc (conversión de documentos) ──
        pandoc \
        # ── Chromium y dependencias para --to webpdf ──
        chromium \
        chromium-driver \
        # ── Fuentes Unicode (evita cuadros vacíos en el PDF) ──
        fonts-dejavu \
        fonts-liberation \
        fonts-noto-color-emoji \
        # ── Utilidades ──
        curl \
        git \
        wget \
        && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

ENV PLAYWRIGHT_BROWSERS_PATH=/usr/bin
ENV PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1
ENV CHROME_BIN=/usr/bin/chromium

RUN pip install --no-cache-dir \
        pyspark==4.2.0 \
        notebook==7.6.2 \
        pandas==2.3.3 \
        "nbconvert[webpdf]" \
        playwright \
        jupyterlab

RUN python -m playwright install chromium || \
    python -m playwright install --with-deps chromium || \
    echo "Playwright usará Chromium del sistema"

WORKDIR /opt

CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]