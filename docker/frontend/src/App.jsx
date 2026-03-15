import html2canvas from "html2canvas";
import { jsPDF } from "jspdf";
import { useEffect, useMemo, useRef, useState } from "react";
import worldMapSvgMarkup from "./assets/world-map.svg?raw";

const API_BASE = "/api";
const STATUS_MESSAGE_TIMEOUT_MS = 4000;
const STATUS_MESSAGE_EXIT_MS = 600;
const PDF_EXPORT_MARGIN_MM = 6;
const PDF_EXPORT_CONTENT_TOP_MM = 6;
const PDF_EXPORT_FOOTER_RESERVE_MM = 9;
const PDF_EXPORT_FRAME_RATIO = (297 - (PDF_EXPORT_MARGIN_MM * 2)) / (210 - PDF_EXPORT_CONTENT_TOP_MM - PDF_EXPORT_FOOTER_RESERVE_MM);
const PDF_EXPORT_FRAME_WIDTH_PX = 1400;
const PDF_EXPORT_FRAME_HEIGHT_PX = Math.round(PDF_EXPORT_FRAME_WIDTH_PX / PDF_EXPORT_FRAME_RATIO);
const projectNamePattern = /^[A-Za-z0-9]{1,10}$/;
const dashboardPalette = ["#166534", "#16a34a", "#65a30d", "#0f766e", "#2563eb"];
const analyticsChartPalette = ["#ef4444", "#f97316", "#f59e0b", "#eab308", "#84cc16", "#22c55e", "#14b8a6", "#06b6d4", "#0ea5e9", "#3b82f6", "#8b5cf6", "#ec4899"];
const SIMPLEMAPS_WORLD_ASPECT_RATIO = 2000 / 857;
const SIMPLEMAPS_WORLD_VIEWBOX_WIDTH = 2000;
const SIMPLEMAPS_WORLD_VIEWBOX_HEIGHT = 857;
const ROBINSON_X = [1, 0.9986, 0.9954, 0.99, 0.9822, 0.973, 0.96, 0.9427, 0.9216, 0.8962, 0.8679, 0.835, 0.7986, 0.7597, 0.7186, 0.6732, 0.6213, 0.5722, 0.5322];
const ROBINSON_Y = [0, 0.062, 0.124, 0.186, 0.248, 0.31, 0.372, 0.434, 0.4958, 0.5571, 0.6176, 0.6769, 0.7346, 0.7903, 0.8435, 0.8936, 0.9394, 0.9761, 1];
const SIMPLEMAPS_WORLD_INNER_MARKUP = worldMapSvgMarkup.match(/<svg[^>]*>([\s\S]*?)<\/svg>/i)?.[1] ?? "";

function App() {
  const [accessJwt, setAccessJwt] = useState("");
  const [accessJwtInput, setAccessJwtInput] = useState("");
  const [activeHeaderView, setActiveHeaderView] = useState("projects");
  const [workspaceStatus, setWorkspaceStatus] = useState({ type: "idle", message: "" });
  const [tokenStatus, setTokenStatus] = useState({ type: "idle", message: "" });
  const [projectStatus, setProjectStatus] = useState({ type: "idle", message: "" });
  const [linkStatus, setLinkStatus] = useState({ type: "idle", message: "" });
  const [dashboardStatus, setDashboardStatus] = useState({ type: "idle", message: "" });
  const [dashboardCards, setDashboardCards] = useState({});
  const [projects, setProjects] = useState([]);
  const [sources, setSources] = useState([]);
  const [activeProjectName, setActiveProjectName] = useState("");
  const [dashboardProjectName, setDashboardProjectName] = useState("");
  const [dashboardSource, setDashboardSource] = useState("");
  const [dashboardCompany, setDashboardCompany] = useState("");
  const [selectedSource, setSelectedSource] = useState("");
  const [selectedCompany, setSelectedCompany] = useState("");
  const [projectName, setProjectName] = useState("");
  const [ingestionJwt, setIngestionJwt] = useState("");
  const [ingestionJwtExpiresAt, setIngestionJwtExpiresAt] = useState("");
  const [isIngestionJwtVisible, setIsIngestionJwtVisible] = useState(false);
  const [isBootstrapping, setIsBootstrapping] = useState(true);
  const [isCreatingToken, setIsCreatingToken] = useState(false);
  const [isCreatingProject, setIsCreatingProject] = useState(false);
  const [deletingProjectName, setDeletingProjectName] = useState("");
  const [deleteProjectTarget, setDeleteProjectTarget] = useState("");
  const [deletingOwnerKey, setDeletingOwnerKey] = useState("");
  const [deleteOwnerTarget, setDeleteOwnerTarget] = useState(null);
  const [deleteDialogInput, setDeleteDialogInput] = useState("");
  const [isLinkingOwner, setIsLinkingOwner] = useState(false);
  const [exitingToastIds, setExitingToastIds] = useState({});
  const previousShowProjectPanel = useRef(false);
  const deleteDialogInputRef = useRef(null);
  const toastVisibleTimersRef = useRef(new Map());
  const toastExitTimersRef = useRef(new Map());

  const tokenMeta = getTokenMeta(ingestionJwt, ingestionJwtExpiresAt);
  const activeProject = projects.find((project) => project.projectName === activeProjectName) || null;
  const activeProjectOwners = activeProject?.owners || [];
  const activeProjectVisibleOwners = useMemo(
    () => (
      selectedSource
        ? activeProjectOwners.filter((owner) => owner.source === selectedSource)
        : []
    ),
    [activeProjectOwners, selectedSource],
  );
  const dashboardProjects = useMemo(
    () => projects.filter((project) => Boolean(project.owners?.length) && hasValidProjectIngestionJwt(project)),
    [projects],
  );
  const dashboardProject = dashboardProjects.find((project) => project.projectName === dashboardProjectName) || null;
  const dashboardProjectOwners = dashboardProject?.owners || [];
  const dashboardProjectTokenMeta = getTokenMeta(dashboardProject?.ingestionJwt || "", dashboardProject?.ingestionJwtExpiresAt || "");
  const dashboardProjectHasValidIngestionJwt = Boolean(dashboardProject?.ingestionJwt && !dashboardProjectTokenMeta.expired);
  const sourceGroups = useMemo(() => getSourceGroups(sources, activeProjectOwners), [sources, activeProjectOwners]);
  const sourceOptions = sourceGroups.map((group) => group.source);
  const companyOptions = selectedSource ? getCompaniesForSource(sourceGroups, selectedSource) : [];
  const dashboardSourceGroups = useMemo(
    () => getOwnedSourceGroups(dashboardProjectOwners),
    [dashboardProjectOwners],
  );
  const dashboardSourceOptions = dashboardSourceGroups.map((group) => group.source);
  const dashboardCompanyOptions = dashboardSource ? getCompaniesForSource(dashboardSourceGroups, dashboardSource) : [];
  const dashboardSelectedOwner = dashboardProjectOwners.find(
    (owner) => owner.source === dashboardSource && owner.company === dashboardCompany,
  ) || null;
  const projectNameError = projectName && !projectNamePattern.test(projectName)
    ? "Use 1-10 letters or numbers only."
    : "";
  const normalizedAccessJwt = normalizeJwt(accessJwtInput) || normalizeJwt(accessJwt);
  const hasValidIngestionJwt = Boolean(ingestionJwt && !tokenMeta.expired);
  const activeProjectTokenMeta = getTokenMeta(activeProject?.ingestionJwt || "", activeProject?.ingestionJwtExpiresAt || "");
  const activeProjectHasValidIngestionJwt = Boolean(activeProject?.ingestionJwt && !activeProjectTokenMeta.expired);
  const workspaceHasValidProjectJwt = projects.some((project) => hasValidProjectIngestionJwt(project));
  const hasProjectDashboards = Boolean(dashboardProjects.length);
  const canCreateProject = Boolean(normalizedAccessJwt && hasValidIngestionJwt && projectNamePattern.test(projectName));
  const canOpenDashboard = hasProjectDashboards;
  const showProjectWorkspace = activeHeaderView === "projects";
  const showJwtPanel = showProjectWorkspace && !isBootstrapping && (!projects.length || !workspaceHasValidProjectJwt);
  const showProjectPanel = showProjectWorkspace && Boolean(workspaceHasValidProjectJwt || hasValidIngestionJwt);
  const showSourcePanel = showProjectWorkspace && Boolean(activeProject && activeProjectHasValidIngestionJwt);
  const showCompanyPanel = showProjectWorkspace && Boolean(activeProject && selectedSource && activeProjectHasValidIngestionJwt);
  const showDashboard = activeHeaderView === "dashboard" && canOpenDashboard;
  const projectsTabLabel = projects.length > 1 ? "Projects" : "Project";
  const dashboardTabLabel = "Analytics";
  const sourcePanelTitle = sourceOptions.length === 1 ? "Source" : "Sources";
  const companyPanelTitle = companyOptions.length === 1 ? "Company" : "Companies";
  const linkedOwnersTitle = activeProjectVisibleOwners.length === 1 ? "Linked Owner" : "Linked Owners";
  const deleteDialog = deleteProjectTarget
    ? {
        type: "project",
        confirmationValue: deleteProjectTarget,
        title: "Type the project name to delete it",
        fieldLabel: "Project name",
        confirmLabel: "Confirm delete",
        projectName: deleteProjectTarget,
      }
    : deleteOwnerTarget
      ? {
          type: "owner",
          confirmationValue: formatDeleteOwnerConfirmationValue(deleteOwnerTarget.projectName, deleteOwnerTarget),
          title: "Type the mapping name to remove it",
          fieldLabel: "Project-Source-Company",
          confirmLabel: "Confirm remove",
          ...deleteOwnerTarget,
        }
      : null;
  const isDeleteDialogSubmitting = deleteDialog?.type === "project"
    ? deletingProjectName === deleteProjectTarget
    : deleteDialog?.type === "owner"
      ? deletingOwnerKey === deleteOwnerTarget?.ownerKey
      : false;
  const canConfirmDelete = Boolean(
    deleteDialog
      && deleteDialogInput === deleteDialog.confirmationValue
      && !isDeleteDialogSubmitting,
  );
  const toastEntries = useMemo(() => {
    const entries = [
      createToastEntry("workspace", workspaceStatus, () => {
        setWorkspaceStatus((current) => (
          current.type === workspaceStatus.type && current.message === workspaceStatus.message
            ? { type: "idle", message: "" }
            : current
        ));
      }),
      createToastEntry("token", tokenStatus, () => {
        setTokenStatus((current) => (
          current.type === tokenStatus.type && current.message === tokenStatus.message
            ? { type: "idle", message: "" }
            : current
        ));
      }),
      createToastEntry("project", projectStatus, () => {
        setProjectStatus((current) => (
          current.type === projectStatus.type && current.message === projectStatus.message
            ? { type: "idle", message: "" }
            : current
        ));
      }),
      createToastEntry("link", linkStatus, () => {
        setLinkStatus((current) => (
          current.type === linkStatus.type && current.message === linkStatus.message
            ? { type: "idle", message: "" }
            : current
        ));
      }),
      createToastEntry("dashboard", dashboardStatus, () => {
        setDashboardStatus((current) => (
          current.type === dashboardStatus.type && current.message === dashboardStatus.message
            ? { type: "idle", message: "" }
            : current
        ));
      }, { analytics: true }),
    ].filter(Boolean);

    Object.entries(dashboardCards).forEach(([ownerKey, entry]) => {
      if (!entry?.status?.message) {
        return;
      }

      const [source = "", company = ""] = ownerKey.split("::");
      entries.push(
        createToastEntry(`owner-${ownerKey}`, entry.status, () => {
          setDashboardCards((current) => {
            const currentEntry = current[ownerKey];
            if (
              !currentEntry
              || currentEntry.status?.type !== entry.status.type
              || currentEntry.status?.message !== entry.status.message
            ) {
              return current;
            }

            return {
              ...current,
              [ownerKey]: {
                ...currentEntry,
                status: { type: "idle", message: "" },
              },
            };
          });
        }, { analytics: true }),
      );
    });

    return entries;
  }, [workspaceStatus, tokenStatus, projectStatus, linkStatus, dashboardStatus, dashboardCards]);

  useEffect(() => {
    void loadBootstrap();
  }, []);

  useEffect(() => {
    const activeIds = new Set(toastEntries.map((entry) => entry.id));

    toastEntries.forEach((entry) => {
      if (toastVisibleTimersRef.current.has(entry.id) || toastExitTimersRef.current.has(entry.id)) {
        return;
      }

      const visibleTimer = window.setTimeout(() => {
        toastVisibleTimersRef.current.delete(entry.id);
        setExitingToastIds((current) => ({ ...current, [entry.id]: true }));

        const exitTimer = window.setTimeout(() => {
          toastExitTimersRef.current.delete(entry.id);
          setExitingToastIds((current) => {
            const next = { ...current };
            delete next[entry.id];
            return next;
          });
          entry.dismiss();
        }, STATUS_MESSAGE_EXIT_MS);

        toastExitTimersRef.current.set(entry.id, exitTimer);
      }, STATUS_MESSAGE_TIMEOUT_MS);

      toastVisibleTimersRef.current.set(entry.id, visibleTimer);
    });

    toastVisibleTimersRef.current.forEach((timer, id) => {
      if (activeIds.has(id)) {
        return;
      }
      window.clearTimeout(timer);
      toastVisibleTimersRef.current.delete(id);
    });

    toastExitTimersRef.current.forEach((timer, id) => {
      if (activeIds.has(id)) {
        return;
      }
      window.clearTimeout(timer);
      toastExitTimersRef.current.delete(id);
      setExitingToastIds((current) => {
        if (!current[id]) {
          return current;
        }
        const next = { ...current };
        delete next[id];
        return next;
      });
    });
  }, [toastEntries]);

  useEffect(() => () => {
    toastVisibleTimersRef.current.forEach((timer) => window.clearTimeout(timer));
    toastExitTimersRef.current.forEach((timer) => window.clearTimeout(timer));
    toastVisibleTimersRef.current.clear();
    toastExitTimersRef.current.clear();
  }, []);

  useEffect(() => {
    setIsIngestionJwtVisible(false);
  }, [ingestionJwt]);

  useEffect(() => {
    if (showProjectPanel && !previousShowProjectPanel.current) {
      setProjectStatus({ type: "idle", message: "" });
    }
    previousShowProjectPanel.current = showProjectPanel;
  }, [showProjectPanel]);

  useEffect(() => {
    if (!canOpenDashboard && activeHeaderView === "dashboard") {
      setActiveHeaderView("projects");
    }
  }, [activeHeaderView, canOpenDashboard]);

  useEffect(() => {
    if (!dashboardProjectName) {
      return;
    }

    const projectStillAvailable = dashboardProjects.some((project) => project.projectName === dashboardProjectName);
    if (!projectStillAvailable) {
      setDashboardProjectName("");
      setDashboardSource("");
      setDashboardCompany("");
    }
  }, [dashboardProjectName, dashboardProjects]);

  useEffect(() => {
    if (!dashboardProject) {
      if (dashboardSource) {
        setDashboardSource("");
      }
      if (dashboardCompany) {
        setDashboardCompany("");
      }
      return;
    }

    if (dashboardSource && !dashboardSourceOptions.includes(dashboardSource)) {
      setDashboardSource("");
      setDashboardCompany("");
      return;
    }

    if (dashboardCompany && !dashboardCompanyOptions.includes(dashboardCompany)) {
      setDashboardCompany("");
    }
  }, [dashboardProject, dashboardSourceOptions, dashboardCompanyOptions, dashboardSource, dashboardCompany]);

  useEffect(() => {
    if (!deleteProjectTarget && !deleteOwnerTarget) {
      return;
    }

    deleteDialogInputRef.current?.focus();
  }, [deleteProjectTarget, deleteOwnerTarget]);

  useEffect(() => {
    if ((!deleteProjectTarget && !deleteOwnerTarget) || isDeleteDialogSubmitting) {
      return;
    }

    function handleKeyDown(event) {
      if (event.key === "Escape") {
        closeDeleteDialog();
      }
    }

    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [deleteProjectTarget, deleteOwnerTarget, isDeleteDialogSubmitting]);

  useEffect(() => {
    if (!showDashboard || !dashboardProject || !dashboardSelectedOwner || !dashboardProjectHasValidIngestionJwt) {
      setDashboardCards({});
      setDashboardStatus({ type: "idle", message: "" });
      return;
    }

    let cancelled = false;
    const owner = dashboardSelectedOwner;
    const ownerKey = getOwnerKey(owner);

    setDashboardStatus({
      type: "pending",
      message: `Loading ${owner.company} analytics...`,
    });
    setDashboardCards((current) => ({
      [ownerKey]: {
        kind: "source",
        analytics: current[ownerKey]?.analytics || createEmptySourceAnalytics(owner),
        status: { type: "pending", message: "Loading..." },
      },
    }));

    void (async () => {
      try {
        const analytics = await apiFetch(
          `/analytics/${encodeURIComponent(owner.source)}/${encodeURIComponent(owner.company)}`,
          {
            method: "GET",
            jwt: dashboardProject.ingestionJwt,
          },
        );
        if (cancelled) {
          return;
        }

        setDashboardCards({
          [ownerKey]: {
            kind: "source",
            analytics,
            status: {
              type: "success",
              message: analytics.totalRecords
                ? `${analytics.totalRecords} records`
                : "Empty",
            },
          },
        });
        setDashboardStatus({
          type: "success",
          message: `Loaded ${owner.company} analytics.`,
        });
      } catch (error) {
        if (cancelled) {
          return;
        }

        setDashboardCards({
          [ownerKey]: {
            kind: "source",
            analytics: createEmptySourceAnalytics(owner),
            status: { type: "error", message: error.message },
          },
        });
        setDashboardStatus({ type: "error", message: error.message });
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [showDashboard, dashboardProject, dashboardProjectHasValidIngestionJwt, dashboardSelectedOwner]);

  async function loadBootstrap({ resetDashboardSelection = false } = {}) {
    setIsBootstrapping(true);

    try {
      const response = await apiFetch("/projects/bootstrap", { method: "GET" });
      const nextProjects = response.projects || [];
      const nextSources = response.sources || [];
      const nextProject = chooseProject(nextProjects, activeProjectName);
      const nextDashboardProjects = nextProjects.filter((project) => Boolean(project.owners?.length) && hasValidProjectIngestionJwt(project));
      const nextDashboardProjectName = resetDashboardSelection
        ? ""
        : nextDashboardProjects.some((project) => project.projectName === dashboardProjectName)
          ? dashboardProjectName
          : "";

      setProjects(nextProjects);
      setSources(nextSources);
      setActiveProjectName(nextProject?.projectName || "");
      setDashboardProjectName(nextDashboardProjectName);
      if (resetDashboardSelection) {
        setDashboardSource("");
        setDashboardCompany("");
        setDashboardCards({});
        setDashboardStatus({ type: "idle", message: "" });
      }

      setAccessJwt(normalizeJwt(response.accessJwt || ""));
      setAccessJwtInput("");
      applyProjectSelection(nextProject);
      setProjectStatus({ type: "idle", message: "" });
      setWorkspaceStatus({ type: "idle", message: "" });
    } catch (error) {
      setWorkspaceStatus({ type: "error", message: error.message });
      setProjects([]);
      setSources([]);
      setActiveProjectName("");
      setDashboardProjectName("");
      setDashboardSource("");
      setDashboardCompany("");
      setAccessJwt("");
      setAccessJwtInput("");
      setIngestionJwt("");
      setIngestionJwtExpiresAt("");
      setProjectStatus({ type: "idle", message: "" });
      applyProjectSelection(null);
    } finally {
      setIsBootstrapping(false);
    }
  }

  function applyProjectSelection(project, { preserveToken = false } = {}) {
    if (!project) {
      if (!preserveToken) {
        setIngestionJwt("");
        setIngestionJwtExpiresAt("");
      }
      setSelectedSource("");
      setSelectedCompany("");
      setDashboardStatus({ type: "idle", message: "" });
      return;
    }

    setIngestionJwt(project.ingestionJwt || "");
    setIngestionJwtExpiresAt(project.ingestionJwtExpiresAt || "");

    const firstOwner = project.owners?.[0] || null;
    if (firstOwner && project.ingestionJwt) {
      setSelectedSource(firstOwner.source);
      setSelectedCompany(firstOwner.company);
      return;
    }

    setSelectedSource("");
    setSelectedCompany("");
    setDashboardStatus({ type: "idle", message: "" });
  }

  async function handleCreateIngestionJwt() {
    const token = normalizedAccessJwt;
    if (!token) {
      setTokenStatus({ type: "error", message: "Enter an access JWT first." });
      return;
    }

    setIsCreatingToken(true);
    setTokenStatus({ type: "pending", message: activeProject ? "Refreshing ingestion JWT..." : "Creating ingestion JWT..." });

    try {
      const response = await apiFetch("/api-key", {
        method: "GET",
        jwt: token,
      });

      setIngestionJwt(response.apiKey);
      setIngestionJwtExpiresAt(response.expiresAt || "");

      const normalizedProjects = await apiFetch("/projects/ingestion-jwt", {
        method: "PUT",
        jwt: token,
        body: { ingestionJwt: response.apiKey },
      });
      const nextProjects = normalizedProjects.projects || [];
      const nextProject = chooseProject(nextProjects, activeProjectName);

      setProjects(nextProjects);
      setActiveProjectName(nextProject?.projectName || "");
      applyProjectSelection(nextProject, { preserveToken: !nextProject });
      setWorkspaceStatus({ type: "idle", message: "" });
      setTokenStatus({ type: "idle", message: "" });
    } catch (error) {
      setTokenStatus({ type: "error", message: error.message });
    } finally {
      setIsCreatingToken(false);
    }
  }

  async function handleCreateProject(event) {
    event.preventDefault();
    if (!canCreateProject) {
      return;
    }

    const token = normalizedAccessJwt;
    setIsCreatingProject(true);
    setProjectStatus({ type: "pending", message: "Creating project..." });

    try {
      const created = await apiFetch("/projects", {
        method: "POST",
        jwt: token,
        body: {
          projectName,
          ingestionJwt,
        },
      });

      setProjects((current) => [...current, created].sort((left, right) => left.projectName.localeCompare(right.projectName)));
      setActiveProjectName(created.projectName);
      setProjectName("");
      applyProjectSelection(created);
      setProjectStatus({ type: "success", message: `Project ${created.projectName} is selected.` });
    } catch (error) {
      setProjectStatus({ type: "error", message: error.message });
    } finally {
      setIsCreatingProject(false);
    }
  }

  function handleProjectSelect(nextProjectName) {
    setActiveProjectName(nextProjectName);
    setLinkStatus({ type: "idle", message: "" });
    const nextProject = projects.find((project) => project.projectName === nextProjectName) || null;
    applyProjectSelection(nextProject);
    if (nextProject) {
      setProjectStatus({ type: "success", message: `Project ${nextProject.projectName} is selected.` });
    }
  }

  function handleDashboardProjectSelect(nextProjectName) {
    setDashboardProjectName(nextProjectName);
    setDashboardSource("");
    setDashboardCompany("");
    setDashboardCards({});
    setDashboardStatus({ type: "idle", message: "" });
  }

  async function handleAnalyticsViewOpen() {
    setActiveHeaderView("dashboard");
    setDashboardProjectName("");
    setDashboardSource("");
    setDashboardCompany("");
    setDashboardCards({});
    setDashboardStatus({ type: "idle", message: "" });
    await loadBootstrap({ resetDashboardSelection: true });
  }

  function handleDashboardSourceSelect(source) {
    setDashboardSource(source);
    setDashboardCompany("");
    setDashboardCards({});
    setDashboardStatus({ type: "idle", message: "" });
  }

  function handleDashboardCompanySelect(company) {
    setDashboardCompany(company);
    setDashboardCards({});
    setDashboardStatus({ type: "idle", message: "" });
  }

  function openDeleteProjectDialog(projectName) {
    const token = normalizedAccessJwt;
    if (!token) {
      setProjectStatus({ type: "error", message: "Access JWT required." });
      return;
    }

    const removedProject = projects.find((project) => project.projectName === projectName) || null;
    if (!removedProject) {
      return;
    }

    setDeleteOwnerTarget(null);
    setDeleteProjectTarget(projectName);
    setDeleteDialogInput("");
    setProjectStatus({ type: "idle", message: "" });
  }

  async function handleDeleteProject() {
    if (!deleteProjectTarget) {
      return;
    }

    const token = normalizedAccessJwt;
    if (!token) {
      setProjectStatus({ type: "error", message: "Access JWT required." });
      return;
    }

    setDeletingProjectName(deleteProjectTarget);
    setProjectStatus({ type: "pending", message: `Deleting ${deleteProjectTarget}...` });

    try {
      await apiFetch(`/projects/${encodeURIComponent(deleteProjectTarget)}`, {
        method: "DELETE",
        jwt: token,
      });

      const nextProjects = projects.filter((project) => project.projectName !== deleteProjectTarget);
      const nextProject = chooseProject(nextProjects, activeProjectName === deleteProjectTarget ? "" : activeProjectName);

      setProjects(nextProjects);
      setActiveProjectName(nextProject?.projectName || "");
      if (dashboardProjectName === deleteProjectTarget) {
        setDashboardProjectName("");
        setDashboardSource("");
        setDashboardCompany("");
      }

      setLinkStatus({ type: "idle", message: "" });
      if (activeProjectName === deleteProjectTarget || !nextProject) {
        applyProjectSelection(nextProject);
      }
      if (!nextProject) {
        setAccessJwtInput("");
        setProjectName("");
      }

      setProjectStatus({ type: "success", message: `Deleted ${deleteProjectTarget}.` });
      closeDeleteDialog({ force: true });
    } catch (error) {
      setProjectStatus({ type: "error", message: error.message });
    } finally {
      setDeletingProjectName("");
    }
  }

  function openDeleteOwnerDialog(owner) {
    if (!activeProject) {
      return;
    }

    const token = normalizedAccessJwt;
    if (!token) {
      setProjectStatus({ type: "error", message: "Access JWT required." });
      return;
    }

    setDeleteProjectTarget("");
    setDeleteOwnerTarget({
      ownerKey: getOwnerKey(owner),
      projectName: activeProject.projectName,
      source: owner.source,
      company: owner.company,
    });
    setDeleteDialogInput("");
    setProjectStatus({ type: "idle", message: "" });
  }

  function closeDeleteDialog({ force = false } = {}) {
    if ((deletingProjectName || deletingOwnerKey) && !force) {
      return;
    }

    setDeleteProjectTarget("");
    setDeleteOwnerTarget(null);
    setDeleteDialogInput("");
  }

  async function handleDeleteProjectOwner() {
    if (!deleteOwnerTarget) {
      return;
    }

    const token = normalizedAccessJwt;
    if (!token) {
      setProjectStatus({ type: "error", message: "Access JWT required." });
      return;
    }

    const currentProject = projects.find((project) => project.projectName === deleteOwnerTarget.projectName) || null;
    if (!currentProject) {
      setProjectStatus({ type: "error", message: "Project not found." });
      closeDeleteDialog({ force: true });
      return;
    }

    setDeletingOwnerKey(deleteOwnerTarget.ownerKey);
    setProjectStatus({ type: "pending", message: `Removing ${deleteOwnerTarget.company}...` });

    try {
      await apiFetch(
        `/projects/${encodeURIComponent(deleteOwnerTarget.projectName)}/owners?source=${encodeURIComponent(deleteOwnerTarget.source)}&company=${encodeURIComponent(deleteOwnerTarget.company)}`,
        {
          method: "DELETE",
          jwt: token,
        },
      );

      const nextOwners = (currentProject.owners || []).filter(
        (candidate) => getOwnerKey(candidate) !== deleteOwnerTarget.ownerKey,
      );
      const sourceStillLinked = nextOwners.some((candidate) => candidate.source === deleteOwnerTarget.source);

      setProjects((current) => current.map((project) => (
        project.projectName === deleteOwnerTarget.projectName
          ? { ...project, owners: nextOwners }
          : project
      )));
      setDashboardCards((current) => {
        const next = { ...current };
        delete next[deleteOwnerTarget.ownerKey];
        return next;
      });

      if (selectedSource === deleteOwnerTarget.source && selectedCompany === deleteOwnerTarget.company) {
        setSelectedCompany("");
      }
      if (selectedSource === deleteOwnerTarget.source && !sourceStillLinked) {
        setSelectedSource("");
        setSelectedCompany("");
      }

      setProjectStatus({ type: "success", message: `Removed ${deleteOwnerTarget.source} / ${deleteOwnerTarget.company}.` });
      closeDeleteDialog({ force: true });
    } catch (error) {
      setProjectStatus({ type: "error", message: error.message });
    } finally {
      setDeletingOwnerKey("");
    }
  }

  function handleSourceSelect(source) {
    setSelectedSource(source);
    setSelectedCompany("");
    setDashboardStatus({ type: "idle", message: "" });
    setLinkStatus({ type: "idle", message: "" });
  }

  async function handleCompanySelect(company) {
    if (!activeProject) {
      return;
    }

    const existingOwner = activeProjectOwners.find((owner) => owner.source === selectedSource && owner.company === company);
    setSelectedCompany(company);

    if (existingOwner) {
      setLinkStatus({ type: "notice", message: "Already linked." });
      return;
    }

    setIsLinkingOwner(true);
    setLinkStatus({ type: "pending", message: "Linking..." });

    try {
      const owner = await apiFetch(`/projects/${encodeURIComponent(activeProject.projectName)}/owners`, {
        method: "POST",
        jwt: normalizedAccessJwt,
        body: {
          source: selectedSource,
          company,
        },
      });

      const nextProject = {
        ...activeProject,
        owners: [...activeProjectOwners, owner].sort(compareOwners),
      };

      setProjects((current) => current.map((project) => (
        project.projectName === nextProject.projectName ? nextProject : project
      )));
      setLinkStatus({ type: "success", message: "Linked." });
    } catch (error) {
      setLinkStatus({ type: "error", message: error.message });
      setSelectedCompany("");
    } finally {
      setIsLinkingOwner(false);
    }
  }

  return (
    <div className="app-shell">
      <div className="ambient-grid" />
      <main className="workspace">
        <header className="chrome">
          <div className="chrome-left">
            <div className="brand-lockup">
              <div className="brand-logo" aria-hidden="true">
                <svg className="brand-logo-art" viewBox="0 0 92 68">
                  <defs>
                    <linearGradient id="brand-shell-gradient" x1="10" y1="10" x2="82" y2="58" gradientUnits="userSpaceOnUse">
                      <stop stopColor="#102739" />
                      <stop offset="1" stopColor="#21435e" />
                    </linearGradient>
                  </defs>
                  <rect x="10" y="10" width="72" height="48" rx="14" fill="url(#brand-shell-gradient)" />
                  <rect x="13.5" y="13.5" width="65" height="41" rx="10.5" fill="none" stroke="rgba(255,255,255,0.12)" />
                  <rect x="22" y="37" width="8" height="11" rx="3" fill="#f3ead7" />
                  <rect x="37" y="27" width="8" height="21" rx="3" fill="#e2d0ad" />
                  <rect x="52" y="20" width="8" height="28" rx="3" fill="#c4a76b" />
                  <rect x="67" y="14" width="8" height="34" rx="3" fill="#8fa8bd" />
                </svg>
              </div>
              <div className="brand-copy">
                <p className="brand-strap">Enterprise Intelligence</p>
                <h1 aria-label="Dashalytics" className="brand-wordmark">Dashalytics</h1>
              </div>
            </div>
          </div>
          <div className="chrome-right">
            <button
              className={`header-tab ${activeHeaderView === "projects" ? "header-tab-active" : ""}`}
              type="button"
              onClick={() => setActiveHeaderView("projects")}
            >
              {projectsTabLabel}
            </button>
            {hasProjectDashboards ? (
              <button
                className={`header-tab ${showDashboard ? "header-tab-active" : ""}`}
                type="button"
                onClick={() => void handleAnalyticsViewOpen()}
                disabled={!canOpenDashboard}
              >
                {dashboardTabLabel}
              </button>
            ) : null}
          </div>
        </header>

        <div className="panel-stack">
          {showJwtPanel ? (
            <section className="stage-panel">
              {!hasValidIngestionJwt ? (
                <>
                  <label className="field">
                    <span>Access JWT</span>
                    <input
                      type="password"
                      value={accessJwtInput}
                      onChange={(event) => setAccessJwtInput(normalizeJwt(event.target.value))}
                      autoComplete="off"
                    />
                  </label>

                  <div className="action-row action-row-single">
                    <button
                      className="primary-pill"
                      type="button"
                      onClick={() => void handleCreateIngestionJwt()}
                      disabled={isCreatingToken || !normalizedAccessJwt}
                    >
                      {isCreatingToken ? "Working..." : activeProject ? "Refresh Ingestion JWT" : "Create Ingestion JWT"}
                    </button>
                  </div>
                </>
              ) : null}
              {hasValidIngestionJwt && !activeProject ? (
                <>
                  <label className="field">
                    <span>Ingestion JWT</span>
                    <div className="field-with-action">
                      <input
                        type={isIngestionJwtVisible ? "text" : "password"}
                        value={ingestionJwt}
                        readOnly
                      />
                      <button
                        className="field-action"
                        type="button"
                        aria-label={isIngestionJwtVisible ? "Hide ingestion JWT" : "Show ingestion JWT"}
                        onClick={() => setIsIngestionJwtVisible((current) => !current)}
                      >
                        <EyeIcon open={isIngestionJwtVisible} />
                        <span>{isIngestionJwtVisible ? "Hide" : "Show"}</span>
                      </button>
                    </div>
                  </label>
                  <p className="helper">Expires at {tokenMeta.expiryLabel}</p>
                </>
              ) : null}
            </section>
          ) : null}

          {showProjectPanel ? (
            <section className="stage-panel">
              <div className="project-list project-primary-panel">
                <PanelHeader title={projectsTabLabel} />
                <div className="project-grid project-primary-grid">
                  <form className="project-form project-form-create" onSubmit={handleCreateProject}>
                    <label className="field">
                      <input
                        type="text"
                        value={projectName}
                        onChange={(event) => setProjectName(event.target.value)}
                        maxLength="10"
                        aria-label="Project name"
                        placeholder="Project name"
                      />
                    </label>
                    <button
                      className="primary-pill project-action-emoji"
                      type="submit"
                      disabled={!canCreateProject || isCreatingProject}
                      aria-label={isCreatingProject ? "Creating project" : "Create project"}
                      title={isCreatingProject ? "Creating project" : "Create project"}
                    >
                      {isCreatingProject ? "⏳" : "✅"}
                    </button>
                    <p className={`helper form-note ${projectNameError ? "helper-error" : ""}`}>
                      {projectNameError || "Alphanumeric, max 10."}
                    </p>
                  </form>

                  {projects.length ? (
                    <div className="project-list">
                      <div className="choice-list">
                        {projects.map((project) => (
                          <div
                            className={`choice-card choice-card-project ${project.projectName === activeProjectName ? "choice-card-active" : ""}`}
                            key={project.projectName}
                          >
                            <button
                              className="choice-card-main"
                              type="button"
                              onClick={() => handleProjectSelect(project.projectName)}
                              disabled={deletingProjectName === project.projectName}
                            >
                              <strong>{project.projectName}</strong>
                              <span>{project.owners?.length || 0} owners</span>
                            </button>
                            <button
                              className="choice-card-delete"
                              type="button"
                              onClick={() => openDeleteProjectDialog(project.projectName)}
                              disabled={Boolean(deletingProjectName)}
                              aria-label={deletingProjectName === project.projectName ? "Deleting project" : `Delete ${project.projectName}`}
                              title={deletingProjectName === project.projectName ? "Deleting project" : `Delete ${project.projectName}`}
                            >
                              {deletingProjectName === project.projectName ? "⏳" : "❌"}
                            </button>
                          </div>
                        ))}
                      </div>
                    </div>
                  ) : null}
                </div>
              </div>

              {showSourcePanel ? (
                <div className="project-selection-shell">
                  <div className="project-list project-selection-panel">
                    <PanelHeader title={sourcePanelTitle} />
                    {sourceOptions.length ? (
                      <div className="radio-card-list">
                        {sourceOptions.map((source) => (
                          <label
                            className={`radio-card ${selectedSource === source ? "radio-card-active" : ""}`}
                            key={source}
                          >
                            <input
                              type="radio"
                              name="source"
                              value={source}
                              checked={selectedSource === source}
                              onChange={() => handleSourceSelect(source)}
                            />
                            <span className="radio-indicator" aria-hidden="true" />
                            <SourceThumbnail source={source} />
                            <span className="radio-copy">
                              <strong>{source}</strong>
                            </span>
                          </label>
                        ))}
                      </div>
                    ) : (
                      <p className="empty-copy">No sources.</p>
                    )}
                  </div>

                  <div className="project-selection-stack">
                    <div className="project-list project-selection-panel">
                      <PanelHeader title={companyPanelTitle} />
                      {selectedSource ? (
                        companyOptions.length ? (
                          <div className="radio-card-list">
                            {companyOptions.map((company) => (
                              <label
                                className={`radio-card ${selectedCompany === company ? "radio-card-active" : ""} ${isLinkingOwner ? "radio-card-disabled" : ""}`}
                                key={company}
                              >
                                <input
                                  type="radio"
                                  name="company"
                                  value={company}
                                  checked={selectedCompany === company}
                                  onChange={() => void handleCompanySelect(company)}
                                  disabled={isLinkingOwner}
                                />
                                <span className="radio-indicator" aria-hidden="true" />
                                <CompanyFavicon
                                  source={selectedSource}
                                  company={company}
                                  websiteDomain={getCompanyWebsiteDomain(sourceGroups, selectedSource, company)}
                                  className="company-favicon-panel"
                                />
                                <span className="radio-copy">
                                  <strong>{company}</strong>
                                </span>
                              </label>
                            ))}
                          </div>
                        ) : (
                          <p className="empty-copy">No companies.</p>
                        )
                      ) : (
                        <p className="empty-copy">Select a source.</p>
                      )}
                    </div>

                    {activeProject && selectedSource && activeProjectVisibleOwners.length ? (
                      <div className="project-list project-linked-panel">
                        <PanelHeader title={linkedOwnersTitle} />
                        <div className="project-owner-row">
                          {activeProjectVisibleOwners.map((owner) => {
                            const ownerKey = getOwnerKey(owner);
                            const isDeletingOwner = deletingOwnerKey === ownerKey;

                            return (
                              <div className="project-owner-company-item" key={ownerKey}>
                                <div className="project-owner-badge">
                                  <CompanyFavicon
                                    source={owner.source}
                                    company={owner.company}
                                    websiteDomain={owner.websiteDomain}
                                    className="company-favicon-panel"
                                  />
                                  <strong>{owner.company}</strong>
                                </div>
                                <button
                                  className="choice-card-delete"
                                  type="button"
                                  onClick={() => openDeleteOwnerDialog(owner)}
                                  disabled={Boolean(deletingOwnerKey)}
                                  aria-label={isDeletingOwner ? "Removing owner" : `Remove ${owner.company}`}
                                  title={isDeletingOwner ? "Removing owner" : `Remove ${owner.company}`}
                                >
                                  {isDeletingOwner ? "⏳" : "❌"}
                                </button>
                              </div>
                            );
                          })}
                        </div>
                      </div>
                    ) : null}
                  </div>
                </div>
              ) : null}
            </section>
          ) : null}
        </div>

        {showDashboard ? (
          <DashboardPanel
            projects={dashboardProjects}
            project={dashboardProject}
            sourceGroups={dashboardSourceGroups}
            selectedSource={dashboardSource}
            selectedCompany={dashboardCompany}
            selectedOwner={dashboardSelectedOwner}
            entry={dashboardSelectedOwner ? dashboardCards[getOwnerKey(dashboardSelectedOwner)] : null}
            onProjectSelect={handleDashboardProjectSelect}
            onSourceSelect={handleDashboardSourceSelect}
            onCompanySelect={handleDashboardCompanySelect}
            onStatusChange={setDashboardStatus}
          />
        ) : null}

        <ToastLayer entries={toastEntries} exitingToastIds={exitingToastIds} />

        {deleteDialog ? (
          <DeleteConfirmationDialog
            canConfirm={canConfirmDelete}
            confirmationValue={deleteDialog.confirmationValue}
            fieldLabel={deleteDialog.fieldLabel}
            inputRef={deleteDialogInputRef}
            isSubmitting={isDeleteDialogSubmitting}
            mode={deleteDialog.type}
            onCancel={closeDeleteDialog}
            onConfirm={deleteDialog.type === "project" ? () => void handleDeleteProject() : () => void handleDeleteProjectOwner()}
            projectName={deleteDialog.projectName}
            confirmLabel={deleteDialog.confirmLabel}
            source={deleteDialog.source}
            company={deleteDialog.company}
            title={deleteDialog.title}
            typedValue={deleteDialogInput}
            onTypedValueChange={setDeleteDialogInput}
          />
        ) : null}
      </main>
    </div>
  );
}

function PanelHeader({ title, detail }) {
  return (
    <div className="panel-header">
      <h3>{title}</h3>
      {detail ? <p>{detail}</p> : null}
    </div>
  );
}

function sanitizeFilenamePart(value) {
  return String(value || "")
    .trim()
    .replace(/[^a-z0-9]+/gi, "-")
    .replace(/^-+|-+$/g, "")
    .toLowerCase();
}

function formatTimestampForFilename(date) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  const hours = String(date.getHours()).padStart(2, "0");
  const minutes = String(date.getMinutes()).padStart(2, "0");
  const seconds = String(date.getSeconds()).padStart(2, "0");
  return `${year}${month}${day}-${hours}${minutes}${seconds}`;
}

function formatLocalDateTimeWithTimeZone(value) {
  const parsed = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return "Unavailable";
  }

  const timeZone = Intl.DateTimeFormat().resolvedOptions().timeZone;
  return timeZone ? `${parsed.toLocaleString()} (${timeZone})` : parsed.toLocaleString();
}

function drawPdfFooter(pdf, pageWidth, pageHeight, margin, exportedAtLabel) {
  pdf.setDrawColor(229, 231, 235);
  pdf.line(margin, pageHeight - 8, pageWidth - margin, pageHeight - 8);
  pdf.setTextColor(107, 114, 128);
  pdf.setFont("helvetica", "normal");
  pdf.setFontSize(8);
  pdf.text(exportedAtLabel, pageWidth - margin, pageHeight - 4.2, { align: "right" });
}

function prepareAnalyticsExportClone(element, exportedAtLabel = "") {
  const clone = element.cloneNode(true);
  clone.classList.add("analytics-export-capture");
  clone.setAttribute("aria-hidden", "true");
  clone.style.position = "fixed";
  clone.style.left = "0";
  clone.style.top = "0";
  clone.style.zIndex = "-1";
  clone.style.pointerEvents = "none";
  clone.style.opacity = "1";
  clone.style.width = `${PDF_EXPORT_FRAME_WIDTH_PX}px`;
  clone.style.height = `${PDF_EXPORT_FRAME_HEIGHT_PX}px`;
  clone.style.overflow = "hidden";
  clone.style.background = "#fffdf6";
  clone.style.color = "#111827";
  clone.style.boxShadow = "none";
  clone.style.filter = "none";

  const nodes = [clone, ...clone.querySelectorAll("*")];
  nodes.forEach((node) => {
    if (node instanceof HTMLElement) {
      node.style.setProperty("color-scheme", "light");
      node.style.setProperty("background-image", "none", "important");
      node.style.setProperty("box-shadow", "none", "important");
      node.style.setProperty("filter", "none", "important");
      node.style.setProperty("backdrop-filter", "none", "important");
      node.style.setProperty("text-shadow", "none", "important");
      node.style.setProperty("outline-color", "#111827", "important");
      node.style.setProperty("text-decoration-color", "#111827", "important");
      node.style.setProperty("caret-color", "transparent", "important");

      if (node.classList.contains("analytics-export-surface")) {
        node.style.setProperty("background-color", "#fffdf6", "important");
        node.style.setProperty("color", "#111827", "important");
      } else if (
        node.classList.contains("analytics-export-meta-card")
        || node.classList.contains("viz-card")
        || node.classList.contains("donut-legend-row")
        || node.classList.contains("route-map-list-row")
        || node.classList.contains("flight-horizontal-row")
      ) {
        node.style.setProperty("background-color", "#ffffff", "important");
        node.style.setProperty("border-color", "#e5e7eb", "important");
        node.style.setProperty("color", "#111827", "important");
      } else if (node.classList.contains("corner-pill")) {
        node.style.setProperty("background-color", "#fff4bf", "important");
        node.style.setProperty("color", "#7c5c00", "important");
      } else if (node.classList.contains("flight-bar-track")) {
        node.style.setProperty("background-color", "#f3f4f6", "important");
      } else if (node.classList.contains("flight-horizontal-track")) {
        node.style.setProperty("background-color", "#e5e7eb", "important");
      }
    }

    if (node instanceof SVGElement) {
      if (node.classList.contains("route-map-surface")) {
        node.setAttribute("fill", "#ffffff");
        node.setAttribute("stroke", "#dbe4f0");
      }
      if (node.classList.contains("line-chart-gridline")) {
        node.setAttribute("stroke", "#dbe4f0");
      }
    }
  });

  clone.querySelectorAll("[data-export-printed-at]").forEach((node) => {
    node.textContent = exportedAtLabel;
  });

  document.body.appendChild(clone);
  return clone;
}

function getAnalyticsExportRowPages(measurementClone) {
  const exportCard = measurementClone.querySelector(".analytics-export-card");
  const grid = measurementClone.querySelector(".flight-grid");
  const gridCards = Array.from(grid?.children || []);

  if (!exportCard || !grid || !gridCards.length) {
    return [[]];
  }

  const cardPaddingBottom = Number.parseFloat(window.getComputedStyle(exportCard).paddingBottom || "0") || 0;
  const availableGridHeight = exportCard.clientHeight - grid.offsetTop - cardPaddingBottom;

  const rows = [];
  gridCards.forEach((cardNode, index) => {
    const top = cardNode.offsetTop;
    const bottom = top + cardNode.offsetHeight;
    const lastRow = rows[rows.length - 1];
    if (lastRow && Math.abs(lastRow.top - top) < 2) {
      lastRow.indices.push(index);
      lastRow.bottom = Math.max(lastRow.bottom, bottom);
      return;
    }

    rows.push({
      top,
      bottom,
      indices: [index],
    });
  });

  const pages = [];
  let currentPage = [];
  let currentHeight = 0;

  rows.forEach((row, index) => {
    const previousRow = index > 0 ? rows[index - 1] : null;
    const rowHeight = row.bottom - row.top;
    const rowGap = currentPage.length && previousRow ? row.top - previousRow.bottom : 0;
    const nextHeight = currentHeight + rowGap + rowHeight;

    if (currentPage.length && nextHeight > availableGridHeight) {
      pages.push(currentPage);
      currentPage = [...row.indices];
      currentHeight = rowHeight;
      return;
    }

    currentPage.push(...row.indices);
    currentHeight = nextHeight;
  });

  if (currentPage.length || !pages.length) {
    pages.push(currentPage);
  }

  return pages;
}

function prepareAnalyticsExportPages(element, exportedAtLabel) {
  const measurementClone = prepareAnalyticsExportClone(element, exportedAtLabel);
  const rowPages = getAnalyticsExportRowPages(measurementClone);
  measurementClone.remove();

  return rowPages.map((visibleCardIndices) => {
    const pageClone = prepareAnalyticsExportClone(element, exportedAtLabel);
    const grid = pageClone.querySelector(".flight-grid");
    const gridCards = Array.from(grid?.children || []);
    if (grid && visibleCardIndices.length) {
      const visibleIndexSet = new Set(visibleCardIndices);
      gridCards.forEach((cardNode, index) => {
        if (!visibleIndexSet.has(index)) {
          cardNode.remove();
        }
      });
    }
    return pageClone;
  });
}

async function exportAnalyticsPdf({ element, projectName, sourceName, companyName }) {
  if (!element) {
    return;
  }

  if (document.fonts?.ready) {
    await document.fonts.ready;
  }
  await new Promise((resolve) => window.requestAnimationFrame(() => resolve()));

  const exportedAt = new Date();
  const exportedAtLabel = formatLocalDateTimeWithTimeZone(exportedAt);
  const exportedAtFilename = formatTimestampForFilename(exportedAt);
  const captureRoots = prepareAnalyticsExportPages(element, exportedAtLabel);
  const canvases = [];
  try {
    for (const captureRoot of captureRoots) {
      const canvas = await html2canvas(captureRoot, {
        backgroundColor: "#fffdf6",
        scale: Math.max(2, window.devicePixelRatio || 1),
        useCORS: true,
        logging: false,
        ignoreElements: (node) => node instanceof HTMLElement && node.classList.contains("chart-tooltip"),
      });
      canvases.push(canvas);
    }
  } finally {
    captureRoots.forEach((captureRoot) => captureRoot.remove());
  }

  const pdf = new jsPDF({
    orientation: "landscape",
    unit: "mm",
    format: "a4",
    compress: true,
  });

  const pageWidth = pdf.internal.pageSize.getWidth();
  const pageHeight = pdf.internal.pageSize.getHeight();
  const margin = PDF_EXPORT_MARGIN_MM;
  const contentTop = PDF_EXPORT_CONTENT_TOP_MM;
  const footerReserve = PDF_EXPORT_FOOTER_RESERVE_MM;
  const availableWidth = pageWidth - (margin * 2);
  const availableHeight = pageHeight - contentTop - footerReserve;
  const renderX = margin;
  const renderY = contentTop;

  canvases.forEach((canvas, index) => {
    if (index > 0) {
      pdf.addPage("a4", "landscape");
    }
    pdf.addImage(canvas.toDataURL("image/png"), "PNG", renderX, renderY, availableWidth, availableHeight, undefined, "FAST");
    drawPdfFooter(pdf, pageWidth, pageHeight, margin, exportedAtLabel);
  });

  pdf.setProperties({
    title: `${projectName} ${sourceName} ${companyName} analytics ${exportedAtFilename}`,
    subject: `${sourceName} analytics`,
    author: "Dashalytics",
  });
  pdf.save(`${sanitizeFilenamePart(projectName)}-${sanitizeFilenamePart(sourceName)}-${sanitizeFilenamePart(companyName)}-analytics-${exportedAtFilename}.pdf`);
}

function ToastLayer({ entries, exitingToastIds }) {
  if (!entries.length) {
    return null;
  }

  return (
    <div className="toast-layer" aria-live="polite" aria-atomic="true">
      {entries.map((entry) => (
        <article
          className={`status-banner toast-card ${entry.analytics ? "toast-card-analytics" : `status-${entry.type}`} ${exitingToastIds[entry.id] ? "toast-card-exiting" : ""}`}
          key={entry.id}
        >
          <p>{entry.message}</p>
        </article>
      ))}
    </div>
  );
}

function DeleteConfirmationDialog({
  confirmationValue,
  confirmLabel,
  fieldLabel,
  inputRef,
  isSubmitting,
  mode,
  onCancel,
  onConfirm,
  projectName,
  title,
  source,
  company,
  typedValue,
  onTypedValueChange,
  canConfirm,
}) {
  return (
    <div className="dialog-backdrop" role="presentation" onClick={isSubmitting ? undefined : onCancel}>
      <section
        aria-labelledby="delete-dialog-title"
        aria-modal="true"
        className="dialog-window stage-panel"
        role="dialog"
        onClick={(event) => event.stopPropagation()}
      >
        <p className="section-label">Confirm removal</p>
        <h3 id="delete-dialog-title">{title}</h3>
        {mode === "project" ? (
          <p className="helper">
            This permanently deletes project <strong>{projectName}</strong> and its linked owner mappings.
          </p>
        ) : (
          <p className="helper">
            This removes <strong>{company}</strong> under <strong>{source}</strong> from project <strong>{projectName}</strong>.
          </p>
        )}

        <div className="dialog-token">
          <span className="micro-copy">Type exactly</span>
          <strong>{confirmationValue}</strong>
        </div>

        <label className="field">
          <span>{fieldLabel}</span>
          <input
            ref={inputRef}
            autoComplete="off"
            onChange={(event) => onTypedValueChange(event.target.value)}
            spellCheck="false"
            type="text"
            value={typedValue}
          />
        </label>

        <div className="dialog-actions">
          <button className="secondary-pill" type="button" onClick={onCancel} disabled={isSubmitting}>
            Cancel
          </button>
          <button className="choice-card-delete" type="button" onClick={onConfirm} disabled={!canConfirm || isSubmitting}>
            {isSubmitting ? (mode === "project" ? "Deleting..." : "Removing...") : confirmLabel}
          </button>
        </div>
      </section>
    </div>
  );
}

function EyeIcon({ open }) {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path
        d="M2 12C4.7 7.7 8.1 5.5 12 5.5S19.3 7.7 22 12c-2.7 4.3-6.1 6.5-10 6.5S4.7 16.3 2 12Z"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.7"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <circle cx="12" cy="12" r="3" fill="none" stroke="currentColor" strokeWidth="1.7" />
      {!open ? <path d="M4 20 20 4" fill="none" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" /> : null}
    </svg>
  );
}

function DashboardPanel({
  projects,
  project,
  sourceGroups,
  selectedSource,
  selectedCompany,
  selectedOwner,
  entry,
  onProjectSelect,
  onSourceSelect,
  onCompanySelect,
  onStatusChange,
}) {
  const projectTokenMeta = getTokenMeta(project?.ingestionJwt || "", project?.ingestionJwtExpiresAt || "");
  const projectIsValid = Boolean(project && !projectTokenMeta.expired);
  const sourcePanelTitle = sourceGroups.length === 1 ? "Source" : "Sources";
  const selectedSourceGroup = sourceGroups.find((group) => group.source === selectedSource) || null;
  const companyPanelTitle = (selectedSourceGroup?.owners.length || 0) === 1 ? "Company" : "Companies";
  const analyticsContentRef = useRef(null);
  const exportContentRef = useRef(null);
  const [isExportingPdf, setIsExportingPdf] = useState(false);
  const canExportAnalytics = Boolean(
    project?.projectName
      && selectedSource
      && selectedOwner?.company
      && entry?.analytics?.charts?.length,
  );

  async function handlePrintAnalytics() {
    if (!exportContentRef.current || isExportingPdf) {
      return;
    }

    setIsExportingPdf(true);
    try {
      await exportAnalyticsPdf({
        element: exportContentRef.current,
        projectName: project?.projectName || "",
        sourceName: selectedSource,
        companyName: selectedOwner?.company || "",
      });
      onStatusChange({ type: "success", message: `Downloaded ${selectedOwner?.company || "analytics"} PDF.` });
    } catch (error) {
      console.error("Analytics PDF export failed", error);
      onStatusChange({ type: "error", message: error.message || "Unable to export PDF." });
    } finally {
      setIsExportingPdf(false);
    }
  }

  return (
    <section className="dashboard-shell">
      <div className="dashboard-layout">
        <div className="dashboard-selector-stack">
          <article className="stage-panel dashboard-selector-panel">
            <PanelHeader title="Name" detail={!project ? "Click on a project" : undefined} />
            {projects.length ? (
              <div className="choice-list">
                {projects.map((candidate) => (
                  <button
                    className={`choice-card choice-card-mainless ${candidate.projectName === project?.projectName ? "choice-card-active" : ""}`}
                    key={candidate.projectName}
                    type="button"
                    onClick={() => onProjectSelect(candidate.projectName)}
                  >
                    <strong>{candidate.projectName}</strong>
                  </button>
                ))}
              </div>
            ) : (
              <p className="empty-copy">No projects.</p>
            )}
            {project ? (
              <div className="dashboard-project-meta">
                <p>Created {formatTimestampLabel(project.createdAt)}</p>
                <p>JWT expiry {projectTokenMeta.expiryLabel}</p>
                <p>Status {projectTokenMeta.expired ? "Expired" : "Valid"}</p>
              </div>
            ) : null}
          </article>

          {projectIsValid ? (
            <article className="stage-panel dashboard-selector-panel">
              <PanelHeader title={sourcePanelTitle} detail={!selectedSource ? "Click on a source" : undefined} />
              {sourceGroups.length ? (
                <div className="dashboard-source-stack">
                  {sourceGroups.map((group) => (
                    <div
                      className={`dashboard-source-subpanel ${group.source === selectedSource ? "dashboard-source-subpanel-active" : ""}`}
                      key={group.source}
                    >
                      <button className="dashboard-source-trigger" type="button" onClick={() => onSourceSelect(group.source)}>
                        <div className="dashboard-source-card">
                          <SourceThumbnail source={group.source} />
                          <strong>{group.source}</strong>
                        </div>
                      </button>
                    </div>
                  ))}
                </div>
              ) : (
                <p className="empty-copy">No sources.</p>
              )}
            </article>
          ) : null}

          {selectedSourceGroup ? (
            <article className="stage-panel dashboard-selector-panel dashboard-company-selector-panel">
              <PanelHeader title={companyPanelTitle} detail={!selectedCompany ? "Click on a company" : undefined} />
              {selectedSourceGroup.owners.length ? (
                <div className="dashboard-company-subgrid">
                  {selectedSourceGroup.owners.map((owner) => (
                    <button
                      className={`dashboard-company-subpanel ${owner.company === selectedCompany ? "dashboard-company-subpanel-active" : ""}`}
                      key={getOwnerKey(owner)}
                      type="button"
                      onClick={() => onCompanySelect(owner.company)}
                    >
                      <CompanyFavicon
                        source={owner.source}
                        company={owner.company}
                        websiteDomain={owner.websiteDomain}
                        className="company-favicon-panel"
                      />
                      <strong>{owner.company}</strong>
                    </button>
                  ))}
                </div>
              ) : (
                <p className="empty-copy">No companies.</p>
              )}
            </article>
          ) : null}
        </div>

        {selectedOwner ? (
          <div className="dashboard-results-stack">
            <DashboardCompanyCard
              owner={selectedOwner}
              entry={entry}
              isSelected={selectedOwner.company === selectedCompany}
              analyticsContentRef={analyticsContentRef}
              exportContentRef={exportContentRef}
              canExportAnalytics={canExportAnalytics}
              isExportingPdf={isExportingPdf}
              onExportPdf={handlePrintAnalytics}
              projectName={project?.projectName || ""}
            />
          </div>
        ) : null}
      </div>
    </section>
  );
}

function DashboardCompanyCard({
  owner,
  entry,
  isSelected,
  analyticsContentRef,
  exportContentRef,
  canExportAnalytics,
  isExportingPdf,
  onExportPdf,
  projectName,
}) {
  const analytics = entry?.analytics || createEmptySourceAnalytics(owner);

  return (
    <article className={`stage-panel dashboard-company-card ${isSelected ? "dashboard-company-card-active" : ""}`}>
      <div className="dashboard-company-heading">
        <div className="dashboard-company-title">
          <CompanyFavicon
            source={owner.source}
            company={owner.company}
            websiteDomain={owner.websiteDomain}
            className="company-favicon-dashboard"
          />
          <h3>{owner.company}</h3>
          {canExportAnalytics ? (
            <button
              className="secondary-pill dashboard-export-pill"
              type="button"
              onClick={() => void onExportPdf()}
              disabled={isExportingPdf}
            >
              {isExportingPdf ? "Exporting..." : "Export PDF"}
            </button>
          ) : null}
        </div>
        <StatusTone status={entry?.status} />
      </div>

      <SourceAnalyticsGrid analytics={analytics} analyticsContentRef={analyticsContentRef} />
      <AnalyticsExportSurface
        analytics={analytics}
        companyName={owner.company}
        exportContentRef={exportContentRef}
        projectName={projectName}
        sourceName={owner.source}
      />
    </article>
  );
}

function AnalyticsExportSurface({
  analytics,
  companyName,
  exportContentRef,
  projectName,
  sourceName,
}) {
  return (
    <div className="analytics-export-surface" ref={exportContentRef} aria-hidden="true">
      <article className="stage-panel dashboard-company-card analytics-export-card">
        <div className="analytics-export-meta">
          <div className="analytics-export-meta-card">
            <span>Project</span>
            <strong>{projectName}</strong>
          </div>
          <div className="analytics-export-meta-card">
            <span>Source</span>
            <strong>{sourceName}</strong>
          </div>
          <div className="analytics-export-meta-card">
            <span>Company</span>
            <strong>{companyName}</strong>
          </div>
          <div className="analytics-export-meta-card">
            <span>Printed</span>
            <strong data-export-printed-at="" />
          </div>
        </div>
        <SourceAnalyticsGrid analytics={analytics} exportMode />
      </article>
    </div>
  );
}

function SourceAnalyticsGrid({ analytics, analyticsContentRef = null, exportMode = false }) {
  return (
    <div className="flight-grid" ref={analyticsContentRef}>
      {(analytics.charts || []).map((chart) => (
        <SourceChartCard
          key={chart.id}
          chart={chart}
          analytics={analytics}
          exportMode={exportMode}
        />
      ))}
    </div>
  );
}

function SourceChartCard({ chart, analytics, exportMode = false }) {
  const items = (chart.items || []).map((item, index) => ({
    ...item,
    color: analyticsChartPalette[index % analyticsChartPalette.length],
    tooltip: item.detail || `${item.label}: ${item.valueLabel || formatChartValue(item.value)}`,
  }));
  const detail = chart.subtitle || `${formatChartValue(analytics.totalRecords || 0)} records`;
  const compactLabels = chart.id.includes("trend") || items.some((item) => item.label.length > 12);

  return (
    <article className="viz-card flight-chart-card">
      <div className="viz-header">
        <div>
          <h3>{chart.title}</h3>
        </div>
        <span className="corner-pill">{detail}</span>
      </div>
      {chart.kind === "route-map" ? <RouteMapChart items={items} exportMode={exportMode} />
        : chart.kind === "donut" ? <InteractiveDonutChart items={items} exportMode={exportMode} />
          : chart.kind === "line" ? <InteractiveLineChart items={items} exportMode={exportMode} />
            : chart.kind === "horizontal-bar" ? <FlightHorizontalBarChart items={items} exportMode={exportMode} />
              : <FlightVerticalBarChart items={items} compactLabels={compactLabels} exportMode={exportMode} />}
    </article>
  );
}

function getPointerTooltipPosition(container, event, offsetX = 12, offsetY = -16) {
  const bounds = container?.getBoundingClientRect();
  if (!bounds) {
    return null;
  }

  return {
    x: event.clientX - bounds.left + offsetX,
    y: event.clientY - bounds.top + offsetY,
  };
}

function getFocusTooltipPosition(container, target, offsetY = -12) {
  const containerBounds = container?.getBoundingClientRect();
  const targetBounds = target?.getBoundingClientRect();
  if (!containerBounds || !targetBounds) {
    return null;
  }

  return {
    x: targetBounds.left - containerBounds.left + (targetBounds.width / 2),
    y: targetBounds.top - containerBounds.top + offsetY,
  };
}

function FlightVerticalBarChart({ items, compactLabels = false, exportMode = false }) {
  const [activeTooltip, setActiveTooltip] = useState(null);
  const chartRef = useRef(null);
  const max = Math.max(...items.map((item) => item.value), 1);

  if (!items.length) {
    return <p className="empty-copy flight-chart-empty">No data.</p>;
  }

  function updateTooltip(item, event) {
    const position = getPointerTooltipPosition(chartRef.current, event);
    if (!position) {
      return;
    }

    setActiveTooltip({ text: item.tooltip, mode: "pointer", ...position });
  }

  function showFocusTooltip(item, event) {
    const position = getFocusTooltipPosition(chartRef.current, event.currentTarget);
    if (!position) {
      return;
    }

    setActiveTooltip({ text: item.tooltip, mode: "focus", ...position });
  }

  return (
    <div
      ref={chartRef}
      className={`flight-vertical-chart ${compactLabels ? "flight-vertical-chart-compact" : ""}`}
    >
      {items.map((item, index) => {
        const content = (
          <>
          <span className="flight-bar-value">{item.valueLabel || formatChartValue(item.value)}</span>
          <span className="flight-bar-track">
            <span className="flight-bar-fill" />
          </span>
          <span className="flight-bar-label">{item.label}</span>
          </>
        );

        return exportMode ? (
          <div
            key={`${item.label}-${index}`}
            className="flight-bar-button"
            style={{
              "--chart-bar-height": `${Math.max((item.value / max) * 100, 6)}%`,
              "--chart-bar-color": item.color,
            }}
          >
            {content}
          </div>
        ) : (
          <button
            key={`${item.label}-${index}`}
            type="button"
            className="flight-bar-button"
            style={{
              "--chart-bar-height": `${Math.max((item.value / max) * 100, 6)}%`,
              "--chart-bar-color": item.color,
            }}
            aria-label={item.tooltip}
            onMouseEnter={(event) => updateTooltip(item, event)}
            onMouseMove={(event) => updateTooltip(item, event)}
            onMouseLeave={() => setActiveTooltip(null)}
            onFocus={(event) => showFocusTooltip(item, event)}
            onBlur={() => setActiveTooltip(null)}
          >
            {content}
          </button>
        );
      })}
      {!exportMode && activeTooltip ? (
        <div
          className={`chart-tooltip chart-tooltip-visible ${activeTooltip.mode === "pointer" ? "chart-tooltip-pointer" : "chart-tooltip-anchored"}`}
          role="tooltip"
          style={{ left: `${activeTooltip.x}px`, top: `${activeTooltip.y}px`, bottom: "auto" }}
        >
          {activeTooltip.text}
        </div>
      ) : null}
    </div>
  );
}

function FlightHorizontalBarChart({ items, exportMode = false }) {
  const max = Math.max(...items.map((item) => item.value), 1);
  const [activeTooltip, setActiveTooltip] = useState(null);
  const chartRef = useRef(null);

  if (!items.length) {
    return <p className="empty-copy flight-chart-empty">No data.</p>;
  }

  function updateTooltip(item, event) {
    const bounds = chartRef.current?.getBoundingClientRect();
    if (!bounds) {
      return;
    }

    setActiveTooltip({
      text: item.tooltip,
      x: event.clientX - bounds.left + 10,
      y: event.clientY - bounds.top - 16,
    });
  }

  function showFocusTooltip(item, event) {
    const bounds = chartRef.current?.getBoundingClientRect();
    const rowBounds = event.currentTarget.getBoundingClientRect();
    if (!bounds) {
      return;
    }

    setActiveTooltip({
      text: item.tooltip,
      x: rowBounds.left - bounds.left + (rowBounds.width / 2),
      y: rowBounds.top - bounds.top - 12,
    });
  }

  return (
    <div className="flight-horizontal-chart" ref={chartRef}>
      {items.map((item, index) => {
        const content = (
          <>
          <span className="flight-horizontal-label">{item.label}</span>
          <span className="flight-horizontal-track">
            <span
              className="flight-horizontal-fill"
              style={{
                width: `${Math.max((item.value / max) * 100, 6)}%`,
                background: item.color,
              }}
            />
          </span>
          <span className="flight-horizontal-value">{item.valueLabel || formatChartValue(item.value)}</span>
          </>
        );

        return exportMode ? (
          <div key={`${item.label}-${index}`} className="flight-horizontal-row">
            {content}
          </div>
        ) : (
          <button
            key={`${item.label}-${index}`}
            type="button"
            className="flight-horizontal-row"
            aria-label={item.tooltip}
            onMouseEnter={(event) => updateTooltip(item, event)}
            onMouseMove={(event) => updateTooltip(item, event)}
            onMouseLeave={() => setActiveTooltip(null)}
            onFocus={(event) => showFocusTooltip(item, event)}
            onBlur={() => setActiveTooltip(null)}
          >
            {content}
          </button>
        );
      })}
      {!exportMode && activeTooltip ? (
        <div
          className="chart-tooltip chart-tooltip-visible chart-tooltip-pointer"
          role="tooltip"
          style={{ left: `${activeTooltip.x}px`, top: `${activeTooltip.y}px`, bottom: "auto" }}
        >
          {activeTooltip.text}
        </div>
      ) : null}
    </div>
  );
}

function InteractiveDonutChart({ items, exportMode = false }) {
  const total = items.reduce((sum, item) => sum + item.value, 0);
  const [activeTooltip, setActiveTooltip] = useState(null);
  const chartRef = useRef(null);
  const size = 220;
  const strokeWidth = 28;
  const radius = 74;
  const circumference = 2 * Math.PI * radius;
  let cumulativeRatio = 0;

  if (!items.length) {
    return <p className="empty-copy flight-chart-empty">No data.</p>;
  }

  function updateTooltip(item, event) {
    const position = getPointerTooltipPosition(chartRef.current, event);
    if (!position) {
      return;
    }

    setActiveTooltip({ text: item.tooltip, mode: "pointer", ...position });
  }

  function showFocusTooltip(item, event) {
    const position = getFocusTooltipPosition(chartRef.current, event.currentTarget);
    if (!position) {
      return;
    }

    setActiveTooltip({ text: item.tooltip, mode: "focus", ...position });
  }

  return (
    <div className="donut-chart-shell">
      <div className="interactive-donut" ref={chartRef}>
        <svg viewBox={`0 0 ${size} ${size}`} aria-hidden="true">
          <circle
            cx={size / 2}
            cy={size / 2}
            r={radius}
            fill="none"
            stroke="#e5e7eb"
            strokeWidth={strokeWidth}
          />
          {items.map((item, index) => {
            const ratio = total > 0 ? item.value / total : 0;
            const dashLength = Math.max(circumference * ratio, 0);
            const dashOffset = circumference * (1 - cumulativeRatio);
            cumulativeRatio += ratio;

            return (
              <circle
                key={`${item.label}-${index}`}
                className="donut-segment"
                cx={size / 2}
                cy={size / 2}
                r={radius}
                fill="none"
                stroke={item.color}
                strokeWidth={strokeWidth}
                strokeLinecap="round"
                strokeDasharray={`${dashLength} ${circumference}`}
                strokeDashoffset={dashOffset}
                transform={`rotate(-90 ${size / 2} ${size / 2})`}
                tabIndex={exportMode ? undefined : 0}
                role={exportMode ? undefined : "img"}
                aria-label={exportMode ? undefined : item.tooltip}
                onMouseEnter={exportMode ? undefined : (event) => updateTooltip(item, event)}
                onMouseMove={exportMode ? undefined : (event) => updateTooltip(item, event)}
                onMouseLeave={exportMode ? undefined : () => setActiveTooltip(null)}
                onFocus={exportMode ? undefined : (event) => showFocusTooltip(item, event)}
                onBlur={exportMode ? undefined : () => setActiveTooltip(null)}
              />
            );
          })}
        </svg>
        <div className="interactive-donut-center">
          <strong>{formatChartValue(total)}</strong>
          <span>total</span>
        </div>
        {!exportMode && activeTooltip ? (
          <div
            className={`chart-tooltip chart-tooltip-visible ${activeTooltip.mode === "pointer" ? "chart-tooltip-pointer" : "chart-tooltip-anchored"}`}
            role="tooltip"
            style={{ left: `${activeTooltip.x}px`, top: `${activeTooltip.y}px`, bottom: "auto" }}
          >
            {activeTooltip.text}
          </div>
        ) : null}
      </div>
      <div className="donut-legend">
        {items.map((item, index) => {
          const content = (
            <>
            <span className="legend-dot" style={{ background: item.color }} />
            <span className="donut-legend-label">{item.label}</span>
            <strong>{item.valueLabel || formatChartValue(item.value)}</strong>
            </>
          );

          return exportMode ? (
            <div className="donut-legend-row" key={`${item.label}-${index}`}>
              {content}
            </div>
          ) : (
            <button
              className="donut-legend-row"
              key={`${item.label}-${index}`}
              type="button"
              onMouseEnter={(event) => updateTooltip(item, event)}
              onMouseMove={(event) => updateTooltip(item, event)}
              onMouseLeave={() => setActiveTooltip(null)}
              onFocus={(event) => showFocusTooltip(item, event)}
              onBlur={() => setActiveTooltip(null)}
            >
              {content}
            </button>
          );
        })}
      </div>
    </div>
  );
}

function InteractiveLineChart({ items, exportMode = false }) {
  const [activeTooltip, setActiveTooltip] = useState(null);
  const chartRef = useRef(null);
  const values = items.map((item) => item.value);
  const max = Math.max(...values, 1);
  const width = 320;
  const height = 170;
  const paddingX = 18;
  const paddingY = 18;
  const plotWidth = width - paddingX * 2;
  const plotHeight = height - paddingY * 2;
  const step = items.length > 1 ? plotWidth / (items.length - 1) : plotWidth;
  const points = items.map((item, index) => {
    const x = paddingX + (index * step);
    const y = height - paddingY - ((item.value / max) * plotHeight);
    return { ...item, x, y };
  });
  const path = points.map((point, index) => `${index === 0 ? "M" : "L"} ${point.x} ${point.y}`).join(" ");

  if (!items.length) {
    return <p className="empty-copy flight-chart-empty">No data.</p>;
  }

  function getTooltipPosition(x, y) {
    const bounds = chartRef.current?.getBoundingClientRect();
    if (!bounds) {
      return { x, y };
    }

    return {
      x: (x / width) * bounds.width,
      y: (y / height) * bounds.height,
    };
  }

  function updatePointerTooltip(item, event) {
    const position = getPointerTooltipPosition(chartRef.current, event);
    if (!position) {
      return;
    }

    setActiveTooltip({ text: item.tooltip, mode: "pointer", ...position });
  }

  return (
    <div className="line-chart-shell">
      <div className="line-chart-canvas" ref={chartRef}>
        <svg viewBox={`0 0 ${width} ${height}`} aria-hidden="true">
          <defs>
            <linearGradient id="analytics-line-gradient" x1="0%" x2="100%" y1="0%" y2="0%">
              <stop offset="0%" stopColor={analyticsChartPalette[0]} />
              <stop offset="25%" stopColor={analyticsChartPalette[3]} />
              <stop offset="50%" stopColor={analyticsChartPalette[6]} />
              <stop offset="75%" stopColor={analyticsChartPalette[9]} />
              <stop offset="100%" stopColor={analyticsChartPalette[11]} />
            </linearGradient>
          </defs>
          {[0.25, 0.5, 0.75, 1].map((ratio) => (
            <line
              key={ratio}
              x1={paddingX}
              x2={width - paddingX}
              y1={height - paddingY - (plotHeight * ratio)}
              y2={height - paddingY - (plotHeight * ratio)}
              className="line-chart-gridline"
            />
          ))}
          <path className="line-chart-path" d={path} />
          {points.map((point, index) => (
            <circle
              key={`${point.label}-${index}-hit`}
              className="line-chart-hit-area"
              cx={point.x}
              cy={point.y}
              r="13"
              fill="transparent"
              tabIndex={exportMode ? undefined : 0}
              role={exportMode ? undefined : "img"}
              aria-label={exportMode ? undefined : point.tooltip}
              onMouseEnter={exportMode ? undefined : (event) => updatePointerTooltip(point, event)}
              onMouseMove={exportMode ? undefined : (event) => updatePointerTooltip(point, event)}
              onMouseLeave={exportMode ? undefined : () => setActiveTooltip(null)}
              onFocus={exportMode ? undefined : () => {
                const position = getTooltipPosition(point.x, point.y);
                setActiveTooltip({ x: position.x, y: position.y - 14, text: point.tooltip, mode: "focus" });
              }}
              onBlur={exportMode ? undefined : () => setActiveTooltip(null)}
            />
          ))}
          {points.map((point, index) => (
            <circle
              key={`${point.label}-${index}-dot`}
              className="line-chart-point"
              cx={point.x}
              cy={point.y}
              r="5.5"
              fill={point.color}
              aria-hidden="true"
              pointerEvents="none"
            />
          ))}
        </svg>
        {!exportMode && activeTooltip ? (
          <div
            className={`chart-tooltip chart-tooltip-visible ${activeTooltip.mode === "pointer" ? "chart-tooltip-pointer" : "chart-tooltip-anchored"}`}
            role="tooltip"
            style={{
              left: `${activeTooltip.x}px`,
              top: `${activeTooltip.y}px`,
              bottom: "auto",
            }}
          >
            {activeTooltip.text}
          </div>
        ) : null}
      </div>
      <div className="line-chart-labels">
        {items.map((item, index) => (
          <span key={`${item.label}-${index}`}>{formatTimeAxisLabel(item.label)}</span>
        ))}
      </div>
    </div>
  );
}

function RouteMapChart({ items, exportMode = false }) {
  const [activeTooltip, setActiveTooltip] = useState(null);
  const canvasRef = useRef(null);
  const width = 360;
  const height = 200;
  const mapFrame = fitAspectRatioFrame(width, height, 12, 14, SIMPLEMAPS_WORLD_ASPECT_RATIO);

  if (!items.length) {
    return <p className="empty-copy flight-chart-empty">No data.</p>;
  }

  function getTooltipPosition(x, y) {
    const bounds = canvasRef.current?.getBoundingClientRect();
    if (!bounds) {
      return { x, y };
    }

    return {
      x: (x / width) * bounds.width,
      y: (y / height) * bounds.height,
    };
  }

  return (
    <div className="route-map-shell">
      <div className="route-map-canvas" ref={canvasRef}>
        <svg viewBox={`0 0 ${width} ${height}`} aria-hidden="true">
          <defs>
            <marker
              id="route-arrowhead"
              markerWidth="2.1"
              markerHeight="2.1"
              refX="1.75"
              refY="1.05"
              orient="auto"
              markerUnits="userSpaceOnUse"
            >
              <path d="M0 0L2.1 1.05L0 2.1Z" fill="currentColor" />
            </marker>
          </defs>
          <rect x="0" y="0" width={width} height={height} rx="18" className="route-map-surface" />
          <g
            className="route-map-world"
            transform={`translate(${mapFrame.x} ${mapFrame.y}) scale(${mapFrame.width / SIMPLEMAPS_WORLD_VIEWBOX_WIDTH} ${mapFrame.height / SIMPLEMAPS_WORLD_VIEWBOX_HEIGHT})`}
            dangerouslySetInnerHTML={{ __html: SIMPLEMAPS_WORLD_INNER_MARKUP }}
          />
          {items.map((item, index) => {
            const from = projectGeoPoint(item.fromLat, item.fromLng, mapFrame);
            const to = projectGeoPoint(item.toLat, item.toLng, mapFrame);
            const midX = (from.x + to.x) / 2;
            const midY = Math.min(from.y, to.y) - Math.max(Math.abs(to.x - from.x) * 0.14, 14);
            const path = `M ${from.x} ${from.y} Q ${midX} ${midY} ${to.x} ${to.y}`;
            const tooltip = { text: item.tooltip, ...getTooltipPosition(midX, midY) };

            return (
              <g key={`${item.label}-${index}`} className="route-map-series" style={{ color: item.color }}>
                <path
                  className="route-map-hit-area"
                  d={path}
                  tabIndex={exportMode ? undefined : 0}
                  role={exportMode ? undefined : "img"}
                  aria-label={exportMode ? undefined : item.tooltip}
                  onMouseEnter={exportMode ? undefined : (event) => {
                    const position = getPointerTooltipPosition(canvasRef.current, event);
                    if (position) {
                      setActiveTooltip({ text: item.tooltip, mode: "pointer", ...position });
                    }
                  }}
                  onMouseMove={exportMode ? undefined : (event) => {
                    const position = getPointerTooltipPosition(canvasRef.current, event);
                    if (position) {
                      setActiveTooltip({ text: item.tooltip, mode: "pointer", ...position });
                    }
                  }}
                  onMouseLeave={exportMode ? undefined : () => setActiveTooltip(null)}
                  onFocus={exportMode ? undefined : () => setActiveTooltip({ text: item.tooltip, mode: "focus", ...tooltip })}
                  onBlur={exportMode ? undefined : () => setActiveTooltip(null)}
                />
                <path
                  className="route-map-arc"
                  d={path}
                  stroke={item.color}
                />
              </g>
            );
          })}
        </svg>
        {!exportMode && activeTooltip ? (
          <div
            className={`chart-tooltip chart-tooltip-visible ${activeTooltip.mode === "pointer" ? "chart-tooltip-pointer" : "chart-tooltip-anchored"}`}
            role="tooltip"
            style={{ left: `${activeTooltip.x}px`, top: `${activeTooltip.y - 10}px`, bottom: "auto" }}
          >
            {activeTooltip.text}
          </div>
        ) : null}
      </div>
      <div className="route-map-list">
        {items.map((item, index) => {
          const content = (
            <>
            <span className="legend-dot" style={{ background: item.color }} />
            <span>{item.label}</span>
            <strong>{item.valueLabel || formatChartValue(item.value)}</strong>
            </>
          );

          return exportMode ? (
            <div className="route-map-list-row" key={`${item.label}-${index}`}>
              {content}
            </div>
          ) : (
            <button
              className="route-map-list-row"
              key={`${item.label}-${index}`}
              type="button"
              onMouseEnter={(event) => {
                const position = getPointerTooltipPosition(canvasRef.current, event);
                if (position) {
                  setActiveTooltip({ text: item.tooltip, mode: "pointer", ...position });
                }
              }}
              onMouseMove={(event) => {
                const position = getPointerTooltipPosition(canvasRef.current, event);
                if (position) {
                  setActiveTooltip({ text: item.tooltip, mode: "pointer", ...position });
                }
              }}
              onMouseLeave={() => setActiveTooltip(null)}
              onFocus={(event) => {
                const position = getFocusTooltipPosition(canvasRef.current, event.currentTarget, -8);
                if (position) {
                  setActiveTooltip({ text: item.tooltip, mode: "focus", ...position });
                }
              }}
              onBlur={() => setActiveTooltip(null)}
            >
              {content}
            </button>
          );
        })}
      </div>
    </div>
  );
}

function CompanyFavicon({ source, company, websiteDomain, className = "" }) {
  const [didFail, setDidFail] = useState(false);
  const domain = resolveCompanyDomain(source, company, websiteDomain);

  if (!domain || didFail) {
    return <span className={`company-favicon company-favicon-fallback ${className}`}>{company.slice(0, 1)}</span>;
  }

  return (
    <img
      className={`company-favicon ${className}`}
      src={`https://www.google.com/s2/favicons?sz=128&domain_url=${encodeURIComponent(`https://${domain}`)}`}
      alt=""
      loading="lazy"
      referrerPolicy="no-referrer"
      aria-hidden="true"
      onError={() => setDidFail(true)}
    />
  );
}

function StatusTone({ status }) {
  if (!status?.message) {
    return null;
  }

  const label = status.type === "pending"
    ? "Loading"
    : status.type === "error"
      ? "Attention"
      : "Ready";

  return <span className={`status-tone status-tone-${status.type}`}>{label}</span>;
}

function SourceThumbnail({ source }) {
  const palette = getSourceThumbnailPalette(source);

  return (
    <span
      className="source-thumbnail"
      style={{ "--thumb-start": palette.start, "--thumb-end": palette.end }}
      aria-hidden="true"
    >
      <SourceGlyph source={source} />
    </span>
  );
}

function SourceGlyph({ source }) {
  switch (source) {
    case "Events":
      return (
        <svg viewBox="0 0 24 24">
          <rect x="4" y="6" width="16" height="14" rx="3" fill="none" stroke="currentColor" strokeWidth="1.8" />
          <path d="M8 4v4M16 4v4M4 10h16" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
          <circle cx="9" cy="14" r="1.2" fill="currentColor" />
          <circle cx="15" cy="14" r="1.2" fill="currentColor" />
        </svg>
      );
    case "News":
      return (
        <svg viewBox="0 0 24 24">
          <path d="M6 6.5h10a2 2 0 0 1 2 2V17a2.5 2.5 0 0 1-2.5 2.5H8.5A2.5 2.5 0 0 1 6 17V6.5Z" fill="none" stroke="currentColor" strokeWidth="1.8" />
          <path d="M9 10h6M9 13h6M9 16h4" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
        </svg>
      );
    case "ECommerce":
      return (
        <svg viewBox="0 0 24 24">
          <path d="M8 9V7a4 4 0 0 1 8 0v2M6 9h12l-1.2 10H7.2L6 9Z" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
        </svg>
      );
    case "Flights":
      return (
        <svg viewBox="0 0 24 24">
          <path d="m3 13 7.2-1.8L20 5l1 1-6.2 6.8L13 20l-1.8.8-.8-6-4-1.8L3 13Z" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
        </svg>
      );
    default:
      return (
        <svg viewBox="0 0 24 24">
          <path d="M6 18V9M12 18V6M18 18v-4" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
        </svg>
      );
  }
}

function MetricCard({ accent, label, value, delta }) {
  return (
    <article className="metric-card">
      <div className="metric-topline">
        <span className="metric-badge" style={{ background: accent }} />
        <div>
          <p>{label}</p>
          <strong>{value}</strong>
        </div>
      </div>
      <div className="metric-footer">
        <span>{delta}</span>
      </div>
    </article>
  );
}

function DonutChart({ segments }) {
  const gradient = segments.length
    ? `conic-gradient(${segments.map((segment) => `${segment.color} 0 ${segment.end}%`).join(", ")})`
    : "conic-gradient(#d8d9df 0 100%)";

  return (
    <div className="donut-chart" style={{ background: gradient }}>
      <div className="donut-center">
        <strong>{segments.reduce((sum, segment) => sum + segment.value, 0)}</strong>
        <span>total</span>
      </div>
    </div>
  );
}

function TrendChart({ points }) {
  const values = points.map((point) => point.value);
  const max = Math.max(...values, 1);
  const width = 280;
  const height = 128;
  const step = points.length > 1 ? width / (points.length - 1) : width;
  const path = points.map((point, index) => {
    const x = index * step;
    const y = height - ((point.value / max) * 96 + 16);
    return `${index === 0 ? "M" : "L"} ${x} ${y}`;
  }).join(" ");

  return (
    <div className="trend-chart">
      <svg viewBox={`0 0 ${width} ${height}`} aria-hidden="true">
        <defs>
          <linearGradient id="trend-gradient" x1="0%" x2="100%" y1="0%" y2="0%">
            <stop offset="0%" stopColor="#166534" />
            <stop offset="100%" stopColor="#22c55e" />
          </linearGradient>
        </defs>
        <path d={path} fill="none" stroke="url(#trend-gradient)" strokeWidth="4" strokeLinecap="round" />
      </svg>
      <div className="trend-labels">
        {points.map((point) => (
          <span key={point.label}>{point.label}</span>
        ))}
      </div>
    </div>
  );
}

async function apiFetch(path, { method, jwt, body }) {
  const headers = {
    ...(body ? { "Content-Type": "application/json" } : {}),
  };
  if (jwt) {
    headers.Authorization = `Bearer ${jwt}`;
  }

  const response = await fetch(`${API_BASE}${path}`, {
    method,
    headers,
    body: body ? JSON.stringify(body) : undefined,
  });

  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    const detail = payload?.errors?.[0]?.message
      || payload?.errors?.[0]?.detail
      || payload?.message
      || payload?.detail
      || `Request failed with status ${response.status}`;
    throw new Error(detail);
  }

  return payload;
}

function chooseProject(projects, activeProjectName) {
  return projects.find((project) => project.projectName === activeProjectName) || projects[0] || null;
}

function getOwnerKey(owner) {
  return `${owner.source}::${owner.company}`;
}

function createToastEntry(id, status, dismiss, options = {}) {
  if (!status?.message) {
    return null;
  }

  return {
    id: `${id}-${status.type}-${status.message}`,
    type: status.type,
    message: status.message,
    dismiss,
    analytics: Boolean(options.analytics),
  };
}

function formatDeleteOwnerConfirmationValue(projectName, owner) {
  return `${projectName}-${owner.source}-${owner.company}`;
}

function getSourceGroups(sourceRecords, owners = []) {
  const groupMap = new Map();

  for (const record of sourceRecords) {
    const key = record.source;
    if (!groupMap.has(key)) {
      groupMap.set(key, new Map());
    }
    const domain = record.websiteDomain?.trim() || "";
    const companyMap = groupMap.get(key);
    companyMap.set(record.company, domain || companyMap.get(record.company) || "");
  }

  for (const owner of owners) {
    const key = owner.source;
    if (!groupMap.has(key)) {
      groupMap.set(key, new Map());
    }
    const domain = owner.websiteDomain?.trim() || "";
    const companyMap = groupMap.get(key);
    companyMap.set(owner.company, domain || companyMap.get(owner.company) || "");
  }

  return [...groupMap.entries()]
    .map(([source, companyMap]) => ({
      source,
      companies: [...companyMap.keys()].sort(),
      websiteDomains: Object.fromEntries(companyMap),
    }))
    .sort((left, right) => left.source.localeCompare(right.source));
}

function getOwnedSourceGroups(owners = []) {
  const groupMap = new Map();

  for (const owner of owners) {
    const key = owner.source;
    if (!groupMap.has(key)) {
      groupMap.set(key, new Map());
    }
    groupMap.get(key).set(owner.company, owner);
  }

  return [...groupMap.entries()]
    .map(([source, ownerMap]) => ({
      source,
      owners: [...ownerMap.values()].sort(compareOwners),
    }))
    .sort((left, right) => left.source.localeCompare(right.source));
}

function getCompaniesForSource(sourceGroups, source) {
  const group = sourceGroups.find((entry) => entry.source === source);
  if (!group) {
    return [];
  }
  if (group.companies) {
    return group.companies;
  }
  return group.owners?.map((owner) => owner.company) || [];
}

function getCompanyWebsiteDomain(sourceGroups, source, company) {
  const group = sourceGroups.find((entry) => entry.source === source);
  if (!group?.websiteDomains) {
    return "";
  }

  return group.websiteDomains[company] || "";
}

function compareOwners(left, right) {
  return `${left.source}:${left.company}`.localeCompare(`${right.source}:${right.company}`);
}

function createEmptySourceAnalytics(owner = null) {
  return {
    source: owner?.source || "",
    company: owner?.company || "",
    totalRecords: 0,
    charts: [],
  };
}

function createEmptyAnalytics({ projectsCount = 0, owner = null } = {}) {
  return buildAnalytics([], { projectsCount, owner });
}

function buildAnalytics(events, { projectsCount, owner }) {
  const now = Date.now();
  const uniqueLocations = new Map();
  const fieldStats = new Map();
  const dayBuckets = createDayBuckets();
  let last24Hours = 0;
  let last7Days = 0;

  for (const event of events) {
    const createdAt = new Date(event.createdAt);
    const age = now - createdAt.getTime();
    if (age <= 24 * 60 * 60 * 1000) {
      last24Hours += 1;
    }
    if (age <= 7 * 24 * 60 * 60 * 1000) {
      last7Days += 1;
    }

    const locationKey = `${event.city}, ${event.state}, ${event.country}`;
    uniqueLocations.set(locationKey, (uniqueLocations.get(locationKey) || 0) + 1);

    const bucketKey = createdAt.toISOString().slice(0, 10);
    if (dayBuckets.has(bucketKey)) {
      dayBuckets.set(bucketKey, dayBuckets.get(bucketKey) + 1);
    }

    const payload = event.payload || {};
    for (const key of Object.keys(payload)) {
      const stat = fieldStats.get(key) || { total: 0, filled: 0 };
      stat.total += 1;
      if (payload[key] !== null && payload[key] !== "") {
        stat.filled += 1;
      }
      fieldStats.set(key, stat);
    }
  }

  const schemaFields = owner?.tableSchema || [];
  const coverageFields = (schemaFields.length ? schemaFields : [...fieldStats.keys()].map((name) => ({ name, required: false })))
    .map((field) => {
      const stat = fieldStats.get(field.name) || { total: events.length, filled: 0 };
      const base = Math.max(stat.total, events.length, 1);
      return {
        name: prettifyFieldName(field.name),
        percent: Math.round((stat.filled / base) * 100),
        required: Boolean(field.required),
      };
    })
    .slice(0, 4);

  const locations = [...uniqueLocations.entries()]
    .sort((left, right) => right[1] - left[1])
    .slice(0, 4)
    .map(([label, value], index, collection) => ({
      label,
      value,
      percent: collection.length ? Math.round((value / events.length) * 100) : 0,
      color: dashboardPalette[index % dashboardPalette.length],
      end: collection.length
        ? Math.round((collection.slice(0, index + 1).reduce((sum, item) => sum + item[1], 0) / Math.max(events.length, 1)) * 100)
        : 100,
    }));

  const trendPoints = [...dayBuckets.entries()].map(([label, value]) => ({
    label: formatDayLabel(label),
    value,
  }));

  const averageCoverage = coverageFields.length
    ? Math.round(coverageFields.reduce((sum, field) => sum + field.percent, 0) / coverageFields.length)
    : 0;

  return {
    topMetrics: {
      projectsCount,
      totalRecords: events.length,
      locationsCount: uniqueLocations.size,
      last24Hours,
      last7Days,
      activeDelta: `${projectsCount ? "+" : ""}${projectsCount}`,
      recordsDelta: `${last24Hours >= 0 ? "+" : ""}${last24Hours}`,
      locationsDelta: `${uniqueLocations.size ? "+" : ""}${uniqueLocations.size}`,
    },
    coverage: {
      overall: averageCoverage,
      fields: coverageFields,
      summary: [
        { label: "Required fields", value: schemaFields.filter((field) => field.required).length, color: dashboardPalette[0] },
        { label: "Optional fields", value: Math.max(schemaFields.length - schemaFields.filter((field) => field.required).length, 0), color: dashboardPalette[1] },
        { label: "Records loaded", value: events.length, color: dashboardPalette[2] },
      ],
    },
    locationBreakdown: {
      segments: locations.length ? locations : [{
        label: "No records yet",
        value: 0,
        percent: 100,
        color: "#d8d9df",
        end: 100,
      }],
      totalLabel: `${uniqueLocations.size} locations`,
    },
    trend: {
      points: trendPoints,
    },
  };
}

function createDayBuckets() {
  const buckets = new Map();
  const now = new Date();

  for (let index = 6; index >= 0; index -= 1) {
    const day = new Date(now);
    day.setDate(now.getDate() - index);
    buckets.set(day.toISOString().slice(0, 10), 0);
  }

  return buckets;
}

function formatDayLabel(dateString) {
  return new Date(`${dateString}T00:00:00Z`).toLocaleDateString(undefined, { weekday: "short" });
}

function prettifyFieldName(name) {
  return name.replace(/_/g, " ");
}

function formatTimestampLabel(value) {
  if (!value) {
    return "Unavailable";
  }

  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return "Unavailable";
  }

  return parsed.toLocaleString();
}

function formatFlightHourLabel(value) {
  if (!value) {
    return "";
  }

  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return "";
  }

  return parsed.toLocaleTimeString([], { hour: "numeric" });
}

function formatChartValue(value) {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return "0";
  }

  if (Number.isInteger(value)) {
    return value.toLocaleString();
  }

  return value.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

function formatTimeAxisLabel(value) {
  if (!value) {
    return "";
  }

  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }

  const hasExplicitHour = /\d{2}:\d{2}$/.test(value) && !value.endsWith("00:00");
  return hasExplicitHour
    ? parsed.toLocaleTimeString([], { hour: "numeric" })
    : parsed.toLocaleDateString([], { month: "short", day: "numeric" });
}

function fitAspectRatioFrame(width, height, insetX, insetY, aspectRatio) {
  const maxWidth = width - (insetX * 2);
  const maxHeight = height - (insetY * 2);
  let frameWidth = maxWidth;
  let frameHeight = frameWidth / aspectRatio;

  if (frameHeight > maxHeight) {
    frameHeight = maxHeight;
    frameWidth = frameHeight * aspectRatio;
  }

  return {
    x: (width - frameWidth) / 2,
    y: (height - frameHeight) / 2,
    width: frameWidth,
    height: frameHeight,
  };
}

function interpolateRobinsonValue(values, latitude) {
  const clampedLatitude = Math.max(0, Math.min(90, latitude));
  const index = Math.floor(clampedLatitude / 5);
  if (index >= values.length - 1) {
    return values[values.length - 1];
  }

  const remainder = (clampedLatitude - (index * 5)) / 5;
  return values[index] + ((values[index + 1] - values[index]) * remainder);
}

function projectGeoPoint(lat, lng, width, height) {
  if (typeof width === "object" && width !== null) {
    const frame = width;
    const projected = projectGeoPoint(lat, lng, frame.width, frame.height);
    return {
      x: frame.x + projected.x,
      y: frame.y + projected.y,
    };
  }

  const safeLat = Number.isFinite(Number(lat)) ? Number(lat) : 0;
  const safeLng = Number.isFinite(Number(lng)) ? Number(lng) : 0;
  const clampedLat = Math.max(-90, Math.min(90, safeLat));
  const clampedLng = Math.max(-180, Math.min(180, safeLng));
  const xScale = interpolateRobinsonValue(ROBINSON_X, Math.abs(clampedLat));
  const yScale = interpolateRobinsonValue(ROBINSON_Y, Math.abs(clampedLat));

  return {
    x: (0.5 + ((clampedLng / 180) * xScale * 0.5)) * width,
    y: (0.5 - (Math.sign(clampedLat) * yScale * 0.5)) * height,
  };
}

function formatDelayValue(value) {
  return `${formatChartValue(value)}m`;
}

function getTokenMeta(token, expiresAt) {
  if (!token) {
    return { expired: false, expiryLabel: "Not created" };
  }

  if (!expiresAt) {
    return { expired: true, expiryLabel: "Missing expiry" };
  }

  const parsed = new Date(expiresAt);
  if (Number.isNaN(parsed.getTime())) {
    return { expired: true, expiryLabel: "Unreadable token" };
  }

  const expired = parsed.getTime() <= Date.now();
  return {
    expired,
    expiryLabel: expired ? `Expired ${parsed.toLocaleString()}` : parsed.toLocaleString(),
  };
}

function hasValidProjectIngestionJwt(project) {
  if (!project?.ingestionJwt || !project.ingestionJwtExpiresAt) {
    return false;
  }

  return !getTokenMeta(project.ingestionJwt, project.ingestionJwtExpiresAt).expired;
}

function getSourceThumbnailPalette(source) {
  switch (source) {
    case "Events":
      return { start: "#f97316", end: "#ef4444" };
    case "News":
      return { start: "#2563eb", end: "#06b6d4" };
    case "ECommerce":
      return { start: "#14b8a6", end: "#22c55e" };
    case "Flights":
      return { start: "#8b5cf6", end: "#ec4899" };
    default:
      return { start: "#64748b", end: "#334155" };
  }
}

function resolveCompanyDomain(source, company, websiteDomain) {
  const trimmed = websiteDomain?.trim();
  if (trimmed) {
    return trimmed.replace(/^https?:\/\//i, "").replace(/\/.*$/, "");
  }
  return "";
}

function normalizeJwt(value) {
  return value.trim().replace(/^Bearer\s+/i, "");
}

export default App;
