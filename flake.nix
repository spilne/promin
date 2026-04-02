{
  description = "Promin — Effect-based pipeline platform";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
            # Runtime
            bun

            # Node ecosystem (needed for some native deps like duckdb)
            nodejs_22

            # Python (for cross-language benchmarks)
            python312
            uv

            # Tools
            git
            jq

            # Native deps for duckdb
            gcc
            gnumake
            cmake
          ];

          shellHook = ''
            echo "promin dev shell"
            echo "  bun: $(bun --version)"
            echo "  node: $(node --version)"
            echo "  python: $(python3 --version)"
            echo "  uv: $(uv --version)"
          '';
        };
      });
}
