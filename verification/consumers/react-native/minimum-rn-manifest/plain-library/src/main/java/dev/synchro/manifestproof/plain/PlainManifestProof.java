package dev.synchro.manifestproof.plain;

public final class PlainManifestProof {
  private PlainManifestProof() {}

  public static boolean usesBuildConfig() {
    return BuildConfig.DEBUG;
  }
}
