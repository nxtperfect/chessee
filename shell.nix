{pkgs ? import <nixpkgs> {}}:
pkgs.mkShell {
  packages = with pkgs; [
    stdenv.cc.cc.lib
    zlib
    (python3.withPackages (p:
      with p; [
        numpy
        pandas
        requests
        zstandard
        pytorch
      ]))
    uv
  ];
  shellHook = ''
    export "LD_LIBRARY_PATH=${pkgs.stdenv.cc.cc.lib}/lib/:/run/opengl-driver/lib/:${pkgs.zlib}"
    echo "Welcome to python 3 dev shell"
    python --version
    uv --version
  '';
}
