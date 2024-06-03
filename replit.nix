{ pkgs }: {
  deps = [
    pkgs.glibcLocales
    pkgs.gitFull
    pkgs.python38Full
    (pkgs.python38Full.withPackages (ps: with ps; [
      flask
      scrapy
      twisted
      requests
      # Add any additional packages here
    ]))
  ];

  env = {
    PYTHON_LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath [
      # Needed for pandas / numpy
      pkgs.stdenv.cc.cc.lib
      pkgs.zlib
      # Needed for pygame
      pkgs.glib
      # Needed for matplotlib
      pkgs.xorg.libX11
      # Flask dependencies
      pkgs.openssl
      pkgs.libffi
      # Scrapy & Twisted dependencies
      pkgs.libxml2
      pkgs.libxslt
      pkgs.libffi
      pkgs.openssl
      pkgs.zlib
    ];
    PYTHONBIN = "${pkgs.python38Full}/bin/python3.8";
    LANG = "en_US.UTF-8";
  };
}
