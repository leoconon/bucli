package com.bucli;

import com.bucli.commands.Get;
import com.bucli.commands.Lifespan;
import com.bucli.commands.Put;
import io.quarkus.picocli.runtime.annotations.TopCommand;
import picocli.CommandLine.Command;

@TopCommand
@Command(mixinStandardHelpOptions = true, subcommands = { Put.class, Lifespan.class, Get.class})
public class MainApp {
}
