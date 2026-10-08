# Corporate CA certificates

Place approved, PEM-encoded corporate root and required intermediate CA certificates
in this directory with `.pem` or `.crt` extensions, one certificate per file.
Verify their fingerprints with your PKI team before building.

Do not place private keys, passwords, or other secrets here. Certificates are
imported into Java's default truststore during the image build. The build fails
if no certificates are supplied. The README itself is excluded from the build.