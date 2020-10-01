package sigmod2021.db.util;

import sigmod2021.db.event.Persistent;
import sigmod2021.event.Event;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

/**
 * Holds global utility methods, useful overall in the project
 *
 */
public final class Util {

    public static final Comparator<Persistent<Event>> TID_COMP = new Comparator<Persistent<Event>>() {

        @Override
        public int compare(Persistent<Event> o1, Persistent<Event> o2) {
            return o1.getId().compareTo(o2.getId());
        }
    };

    /** Only static methods */
    private Util() {
    }

    /**
     * Deletes the given directory recursively
     * @param dir The directory to delete
     * @throws IOException On any error deleting the given directory
     */
    public static void deleteDirectoryRecursively(final Path dir) throws IOException {
        if (Files.exists(dir)) {
            Files.walk(dir).sorted(Comparator.reverseOrder()).forEach(t -> {
                try {
                    Files.delete(t);
                } catch (final IOException e) {
                }
            });
        }
    }
}
