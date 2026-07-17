import { useState } from "react";
import { useTranslation } from "react-i18next";
import { ForwardedIconComponent } from "@/components/common/genericIconComponent";
import ShadTooltip from "@/components/common/shadTooltipComponent";
import { Button } from "@/components/ui/button";
import type { MCPTransport } from "@/controllers/API/queries/mcp/use-patch-install-mcp";
import { toSpaceCase } from "@/utils/stringManipulation";
import { cn, getOS } from "@/utils/utils";
import {
  autoInstallers,
  buildClaudeInstallCommand,
} from "../utils/mcpServerUtils";

interface McpAutoInstallContentProps {
  isLocalConnection: boolean;
  installedMCPData?: Array<{ name?: string; available?: boolean }>;
  loadingMCP: string[];
  installClient: (
    name: string,
    title?: string,
    transport?: MCPTransport,
  ) => void;
  installedClients?: string[];
  mcpJson?: string;
}

export const McpAutoInstallContent = ({
  isLocalConnection,
  installedMCPData,
  loadingMCP,
  installClient,
  installedClients,
  mcpJson,
}: McpAutoInstallContentProps) => {
  const { t } = useTranslation();
  const [commandCopied, setCommandCopied] = useState(false);
  const installCommand =
    !isLocalConnection && mcpJson
      ? buildClaudeInstallCommand(mcpJson, getOS())
      : null;

  const copyInstallCommand = () => {
    if (!installCommand) return;
    navigator.clipboard.writeText(installCommand);
    setCommandCopied(true);
    setTimeout(() => setCommandCopied(false), 2000);
  };

  return (
    <div className="flex flex-col gap-1">
      {!isLocalConnection && (
        <div className="mb-2 rounded-md bg-accent-amber px-3 py-2 text-sm text-accent-amber-foreground">
          <div className="flex items-center gap-3">
            <ForwardedIconComponent
              name="AlertTriangle"
              className="h-4 w-4 shrink-0"
            />
            <span>{t("mcp.installDisabledWarning")}</span>
          </div>
        </div>
      )}
      {installCommand && (
        <div
          className="mb-2 rounded-md border border-border px-3 py-2"
          data-testid="claude_install_command_panel"
        >
          <div className="mb-2 text-sm">
            Connect <b>Claude Desktop on your computer</b> to this deployed
            server: copy this command, paste it into your Terminal, then restart
            Claude Desktop.
          </div>
          <div className="flex items-center gap-2">
            <code className="block max-h-16 flex-1 overflow-hidden text-ellipsis whitespace-nowrap rounded bg-muted px-2 py-1.5 font-mono text-xs text-muted-foreground">
              {installCommand}
            </code>
            <Button
              variant="outline"
              size="sm"
              onClick={copyInstallCommand}
              data-testid="copy_claude_install_command"
            >
              <ForwardedIconComponent
                name={commandCopied ? "Check" : "TerminalSquare"}
                className="mr-1.5 h-4 w-4"
              />
              {commandCopied ? "Copied" : "Copy command"}
            </Button>
          </div>
        </div>
      )}
      {autoInstallers.map((installer) => (
        <ShadTooltip
          key={installer.name}
          content={
            !installedMCPData?.find((client) => client.name === installer.name)
              ?.available
              ? t("mcp.installTooltip", { name: toSpaceCase(installer.name) })
              : ""
          }
          side="left"
        >
          <div className="w-full flex">
            <Button
              variant="ghost"
              className="group flex flex-1 items-center justify-between disabled:text-foreground disabled:opacity-50"
              disabled={
                loadingMCP.includes(installer.name) ||
                !isLocalConnection ||
                !installedMCPData?.find(
                  (client) => client.name === installer.name,
                )?.available
              }
              onClick={() =>
                installClient(
                  installer.name,
                  installer.title,
                  installer.transport,
                )
              }
            >
              <div className="flex items-center gap-4 text-sm font-medium">
                <ForwardedIconComponent
                  name={installer.icon}
                  className={cn("h-5 w-5")}
                  aria-hidden="true"
                />
                {installer.title}
              </div>
              <div className="relative h-4 w-4">
                <ForwardedIconComponent
                  name={
                    installedClients?.includes(installer.name)
                      ? "Check"
                      : loadingMCP.includes(installer.name)
                        ? "Loader2"
                        : "Plus"
                  }
                  className={cn(
                    "h-4 w-4 absolute top-0 left-0 opacity-100",
                    loadingMCP.includes(installer.name) && "animate-spin",
                    installedClients?.includes(installer.name) &&
                      "group-hover:opacity-0",
                  )}
                />
                {installedClients?.includes(installer.name) && (
                  <ForwardedIconComponent
                    name={"RefreshCw"}
                    className={cn(
                      "h-4 w-4 absolute top-0 left-0 opacity-0 group-hover:opacity-100",
                    )}
                  />
                )}
              </div>
            </Button>
          </div>
        </ShadTooltip>
      ))}
    </div>
  );
};
