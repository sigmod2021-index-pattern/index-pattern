package sigmod2021.pattern.experiments.util;

import sigmod2021.db.core.primaryindex.impl.legacy.ImmutableParams;
import sigmod2021.db.core.primaryindex.impl.legacy.MutableParams;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 *
 */
public class ExperimentUtil {

    public static final String BASE_PATH_ENV = "EXPERIMENTS_BASE_PATH";

    public static ExperimentConfig getConfig() {
        String bp = System.getenv(BASE_PATH_ENV);
        if (bp == null) {
            throw new RuntimeException(BASE_PATH_ENV + " environment variable not set");
        }

        Path p = Paths.get(bp);
        if (!Files.exists(p)) {
            try {
                Files.createDirectories(p);
            } catch (IOException e) {
                throw new RuntimeException("Could not create base directory at: " + p);
            }
        } else if (!Files.isDirectory(p)) {
            throw new RuntimeException("Specified base directory is not a directory: " + p);
        }

        ImmutableParams iParams = new ImmutableParams();
        MutableParams mParams = new MutableParams();

        iParams.setBlockSize(8192);
        iParams.setContainerType(ImmutableParams.ContainerType.BLOCK);
        mParams.setUseDirectIO(false);
        mParams.setUseBlockBuffer(false);
        return new ExperimentConfig(p, iParams, mParams);
    }

    public static class ExperimentConfig {
        public final ImmutableParams iParams;
        public final MutableParams mParams;
        public final Path basePath;

        private ExperimentConfig(Path basePath, ImmutableParams iParams, MutableParams mParams) {
            this.basePath = basePath;
            this.iParams = iParams;
            this.mParams = mParams;
        }
    }


}
