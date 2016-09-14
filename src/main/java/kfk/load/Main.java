package kfk.load;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

import java.util.Optional;
import java.util.function.Consumer;

/**
 */
public class Main {
    public static void main(String[] arguments) {
        final Consumer<Exception> exceptionHandler = Main::handleException;
        final Consumer<Integer> exitHandler = System::exit;

        final Args args = new Args();
        final JCommander parser = new JCommander(args);
        try {
            parser.parse(arguments);
        } catch (ParameterException e) {
            printUsageAndExit(parser, exitHandler, Optional.of("Parameter error: " + e.getMessage()));
        } catch (Exception e) {
            printUsageAndExit(parser, exitHandler, Optional.of("Unknown error: " + e.getMessage()));
        }

        try {
            args.verify();
        } catch (Exception e) {
            printUsageAndExit(parser, exitHandler, Optional.of("Verification failed: " + e.getMessage()));
        }

        if (args.help()) {
            printUsageAndExit(parser, exitHandler, Optional.empty());
        }


        try (final LoadGenerator loadGenerator = new LoadGenerator(args)) {
            loadGenerator.start();
        } catch (Exception e) {
            exceptionHandler.accept(e);
        }
    }

    private static void printUsageAndExit(JCommander parser, Consumer<Integer> exitHandler,
                                          Optional<String> optionalMessage) {
        if (optionalMessage.isPresent()) {
            JCommander.getConsole().println(optionalMessage.get());
        }
        parser.usage();
        exitHandler.accept(1);
    }

    private static void handleException(final Exception ex) {
        ex.printStackTrace();
    }

}
