package nextflow.util


/**
 * @author : jorge <jorge.aguilera@seqera.io>
 *
 */
class NullablePath implements CharSequence{

    String path

    @Override
    String toString() {
        return path
    }

    @Override
    int length() {
        path.length()
    }

    @Override
    char charAt(int index) {
        path.charAt(index)
    }

    @Override
    CharSequence subSequence(int start, int end) {
        path.subSequence(start, end)
    }
}
