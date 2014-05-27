package com.stratio.streaming.shell.renderer;

import org.springframework.shell.ShellException;

public interface Renderer<POJO> {

    public String render(POJO pojo) throws ShellException;

}
