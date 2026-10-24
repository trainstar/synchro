package dev.synchro.manifestproof.react;

public final class ReactManifestProof {
  private ReactManifestProof() {}

  public static boolean usesBuildConfig() {
    return BuildConfig.DEBUG;
  }
}
