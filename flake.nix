{
  description = "continuous-ssh (xssh) — resumable SSH sessions";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs = { self, nixpkgs }:
    let
      # Systems we build for.
      systems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});
    in
    {
      packages = forAllSystems (pkgs: rec {
        xssh = pkgs.buildGoModule {
          pname = "xssh";
          version = "0.1.0";

          src = self;

          # Pinned via go.sum; bump when dependencies change.
          vendorHash = "sha256-09RA3v+50saml1pJuhzVNPhjZF24tAzIPD7DrRL5pKA=";

          subPackages = [ "cmd/xssh" ];

          # Install shell completions alongside the binary.
          postInstall = ''
            installShellCompletion \
              --bash completions/bash/xssh \
              --zsh completions/zsh/_xssh
          '';

          nativeBuildInputs = [ pkgs.installShellFiles ];

          meta = with pkgs.lib; {
            description = "Resumable SSH sessions (xssh)";
            homepage = "https://github.com/zziigguurraatt/continuous-ssh";
            license = licenses.mit;
            mainProgram = "xssh";
          };
        };

        default = xssh;
      });

      apps = forAllSystems (pkgs: rec {
        xssh = {
          type = "app";
          program = "${self.packages.${pkgs.stdenv.hostPlatform.system}.xssh}/bin/xssh";
        };
        default = xssh;
      });
    };
}
